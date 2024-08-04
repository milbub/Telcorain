import hashlib
from io import BytesIO
import os
from PIL import Image
from typing import cast, Iterable, Optional

import numpy as np
import requests
from scipy.ndimage import label

from handlers import config_handler


BLACK_INDEX = [0]  # Upper text color
RED_INDEX = [122, 123, 124, 125, 126, 141, 162, 166, 167, 182, 183, 184, 185, 186, 187, 212, 216]  # Bottom text color
GREY_INDEX = [242]  # Unknown area color

# Maximum number of history fetch steps attempts
MAX_HISTORY_LOOKUPS = int(config_handler.read_option("external_filter", "max_history_lookups"))
# Prefix of the image filenames
FILENAME_PREFIX = config_handler.read_option("external_filter", "file_prefix")
# Directory where cached images will be stored
CACHE_DIR = config_handler.read_option("directories", "ext_filter_cache")


if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)


def _get_cache_path(dt64: np.datetime64, img_url: str) -> str:
    """
    Generate a unique file path for caching the image.
    """
    # Convert date to string format
    date_str = dt64.astype("datetime64[D]").astype(str)
    # Create a hash of the image URL
    url_hash = hashlib.md5(img_url.encode("utf-8")).hexdigest()
    # Combine date and hash to form a unique filename
    filename = f"{date_str}_{url_hash}.img"
    return os.path.join(CACHE_DIR, filename)


def _fetch_image(dt64: np.datetime64, img_url: str, url_prefix: str) -> Optional[bytes]:
    date_str = dt64.astype("datetime64[D]").astype(str)
    year, month, day = date_str.split("-")

    url = f"{url_prefix}/{year}/{str(int(month))}/{str(int(day))}/{img_url}"

    # Check if the image is already cached
    cache_path = _get_cache_path(dt64, url)
    if os.path.exists(cache_path):
        with open(cache_path, "rb") as file:
            return file.read()

    # If not cached, fetch the image
    response: requests.Response = requests.get(url)
    if response.status_code == 200:
        image_data: bytes = response.content
        # Cache the image
        with open(cache_path, "wb") as file:
            file.write(image_data)
        return image_data
    else:
        return None


def _detect_active_pixels(
        img_bytes: bytes,
        point_x: float,
        point_y: float,
        radius_km: float,
        pixel_threshold: int,
        img_x_min: float,
        img_x_max: float,
        img_y_min: float,
        img_y_max: float
) -> bool:
    """
    Check if there are colored pixels in cluster in the specified area around the given point in the map.

    :param img_bytes:
    :param point_x:
    :param point_y:
    :param radius_km:
    :param pixel_threshold:
    :param img_x_min:
    :param img_x_max:
    :param img_y_min:
    :param img_y_max:
    :return:
    """
    img = Image.open(BytesIO(img_bytes)).convert("P")
    transparency = img.info.get("transparency", None)
    pixels = np.array(cast(Iterable, img))

    # Correctly map geographical coordinates to image coordinates
    scale_x = pixels.shape[1] / (img_x_max - img_x_min)
    # Y-axis is flipped because image coordinates start from top-left corner
    scale_y = pixels.shape[0] / (img_y_max - img_y_min)

    # Adjusting for correct mapping of longitude and latitude to image coordinates
    point_px_x = int((point_x - img_x_min) * scale_x)
    # Y-axis coordinate needs to be inverted because higher latitudes are lower on the image
    point_px_y = pixels.shape[0] - int((point_y - img_y_min) * scale_y)

    diagonal_km_per_pixel = np.sqrt(((img_x_max - img_x_min) * 111) ** 2 +
                                    ((img_y_max - img_y_min) * 111) ** 2) / np.sqrt(
        pixels.shape[0] ** 2 + pixels.shape[1] ** 2)
    radius_px = int(radius_km / diagonal_km_per_pixel)

    excluded_indices = [*BLACK_INDEX, *RED_INDEX, *GREY_INDEX, transparency]
    mask = np.isin(pixels, excluded_indices, invert=True).astype(int)

    labeled_array, num_features = label(mask)

    # Creating a search area mask based on the correct coordinates
    search_area_mask = np.zeros_like(pixels, dtype=bool)
    for y in range(pixels.shape[0]):
        for x in range(pixels.shape[1]):
            if (x - point_px_x) ** 2 + (y - point_px_y) ** 2 <= radius_px ** 2:
                search_area_mask[y, x] = True

    labels_in_radius = np.unique(labeled_array[search_area_mask])
    labels_in_radius = labels_in_radius[labels_in_radius != 0]

    for label_number in labels_in_radius:
        cluster_size = np.sum(labeled_array == label_number)
        if cluster_size >= pixel_threshold:
            return True

    return False


def determine_wet(
        sample_timestamp: np.datetime64,
        point_x: float,
        point_y: float,
        radius_km: float,
        pixel_threshold: int,
        img_x_min: float,
        img_x_max: float,
        img_y_min: float,
        img_y_max: float,
        url_prefix: str,
        default_return: bool = True,
        forward_look: bool = False
) -> bool:
    def timestamp_to_filename(ts: np.datetime64):
        """Helper function to format the numpy datetime64 timestamp into image filename format."""
        return f"{FILENAME_PREFIX}{ts.astype('datetime64[m]').astype(str).replace('T', '_').replace(':', '-')}.png"

    delta_10 = np.timedelta64(10, "m")

    # Convert to minutes and round down to the nearest multiple of 10
    minutes_since_epoch = sample_timestamp.astype("datetime64[m]").astype(int)
    rounded_minutes_since_epoch = (minutes_since_epoch // 10) * 10
    lower_timestamp = np.datetime64("1970-01-01T00:00:00") + np.timedelta64(int(rounded_minutes_since_epoch), "m")

    history_lookup = 1
    img_raw = _fetch_image(lower_timestamp, timestamp_to_filename(lower_timestamp), url_prefix)

    while img_raw is None and history_lookup < MAX_HISTORY_LOOKUPS:
        lower_timestamp -= delta_10
        history_lookup += 1
        img_raw = _fetch_image(lower_timestamp, timestamp_to_filename(lower_timestamp), url_prefix)

    if img_raw is not None:
        prev_wet_state = _detect_active_pixels(
            img_raw, point_x, point_y, radius_km, pixel_threshold, img_x_min, img_x_max, img_y_min, img_y_max
        )
    else:
        prev_wet_state = default_return

    if forward_look:
        higher_timestamp = lower_timestamp + (delta_10 * history_lookup)
        img_raw = _fetch_image(higher_timestamp, timestamp_to_filename(higher_timestamp), url_prefix)

        if img_raw is not None:
            next_wet_state = _detect_active_pixels(
                img_raw, point_x, point_y, radius_km, pixel_threshold, img_x_min, img_x_max, img_y_min, img_y_max
            )
        else:
            next_wet_state = default_return

        del img_raw
        return prev_wet_state or next_wet_state
    else:
        del img_raw
        return prev_wet_state


def __get_color_indices(img_path):
    """
    Currently not used, but might be useful in the future. Colors are defined by constants instead.

    Get the color indices for black, red, and grey colors in the image.
    """
    img = Image.open(img_path)
    palette = img.getpalette()  # Each 3 entries represent the RGB values of a color

    # Reshape the palette to make it easier to work with
    palette_array = np.array(palette).reshape(-1, 3)

    # Find the index for black, red, and the specific shade of grey (196,196,196)
    black_index = np.where(np.all(palette_array == [0, 0, 0], axis=1))[0][0]
    red_indices = np.where((palette_array[:, 0] > palette_array[:, 1]) &
                           (palette_array[:, 0] > palette_array[:, 2]) &
                           (palette_array[:, 0] > 100))[0]
    specific_grey_index = np.where(np.all(palette_array == [196, 196, 196], axis=1))[0]

    return black_index, red_indices, specific_grey_index
