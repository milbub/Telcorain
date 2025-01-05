import math

import numpy as np
import xarray as xr

from lib.sweep_intersector.lib.SweepIntersector import SweepIntersector

from handlers.logging_handler import logger


def process_segments(
        calc_data: list[xr.Dataset],
        segment_size: int,
        is_central_enabled: bool,
        is_intersection_enabled: bool,
        log_run_id: str
):
    """
    Process segmentation of CMLs based on the selected method (central points, linear segments, intersection algorithm).

    Central points = one point in the middle of the CML.
    Linear segments = divide the CML into segments of the same length.
    Intersection algorithm = divide the CML into segments based on intersections with other CMLs, if no intersection is
                             found for given CML, apply central points or linear segments method.

    :param calc_data: List of CML datasets to be processed.
    :param segment_size: Size of the segment (in meters).
    :param is_central_enabled: If True, central points method is selected.
    :param is_intersection_enabled: If True, intersection algorithm is selected.
    :param log_run_id: ID of the current calculation run.
    """
    if not is_intersection_enabled:
        if is_central_enabled:
            # apply central points for all links
            logger.debug("[%s] Intersection disabled, central points method is selected.", log_run_id)
            logger.debug("[%s] Calculating central points of all links...", log_run_id)
            for cml in calc_data:
                central_points(cml)
        else:
            # divide all links into linear segments
            logger.debug("[%s] Intersection disabled, linear segments method is selected.", log_run_id)
            logger.debug(
                "[%s] Dividing all links into linear segments with segment size of %d m...",
                log_run_id,
                segment_size
            )
            for cml in calc_data:
                linear_repeat(cml, segment_size)
    else:
        # create list of CML border segment points (= CML coordinates) for intersection algorithm
        cmls_segment_points: list[tuple[tuple[float, float], tuple[float, float]]] = []
        for cml in calc_data:
            cmls_segment_points.append(
                (
                    (float(cml.site_a_longitude.data), float(cml.site_a_latitude.data)),
                    (float(cml.site_b_longitude.data), float(cml.site_b_latitude.data))
                )
            )
        # find intersections of cmls with other cmls
        logger.debug("[%s] Intersection enabled, finding intersections...", log_run_id)
        intersector = SweepIntersector()
        intersections = intersector.findIntersections(cmls_segment_points)
        logger.debug("[%s] Found %d intersections.", log_run_id, len(intersections))

        # for links with no intersections, just apply central points or divide them into segments
        if is_central_enabled:
            logger.debug("[%s] Calculating central points for no-intersecting links...", log_run_id)
        else:
            logger.debug(
                "[%s] Dividing no-intersecting links into linear segments with segment size of %d m...",
                log_run_id,
                segment_size
            )

        for cml in calc_data:
            if (
                (
                    (float(cml.site_a_longitude.data), float(cml.site_a_latitude.data)),
                    (float(cml.site_b_longitude.data), float(cml.site_b_latitude.data))
                )
            ) in intersections:
                continue
            else:
                if is_central_enabled:
                    central_points(cml)
                else:
                    linear_repeat(cml, segment_size)

        # if there are intersections, divide links into segments based on intersection algorithm
        if len(intersections) > 0:
            logger.debug(
                "[%s] Dividing intersecting links into segments based on intersection algorithm...",
                log_run_id
            )
            intersection_algorithm(calc_data, intersections)


def central_points(cml: xr.Dataset):
    """
    Calculate central point of the CML and assign it as the only segment point.

    :param cml: CML dataset to be processed.
    """
    lat_center = (cml.site_a_latitude + cml.site_b_latitude) / 2
    lon_center = (cml.site_a_longitude + cml.site_b_longitude) / 2

    cml["segment_points"] = [1] # only one segment point = the central point
    cml["long_array"] = ("segment_points", [lon_center])
    cml["lat_array"] = ("segment_points", [lat_center])
    cml["cml_reference"] = ("segment_points", [int(cml.cml_id.data)]) # reference to the same CML = use own rain values


def linear_repeat(cml: xr.Dataset, segment_size: int):
    """
    Divide the CML into segments of the same length.

    :param cml: CML dataset to be processed.
    :param segment_size: Size of the segment (in meters).
    """
    # putting coordinates into variables
    site_a = {"x": cml.site_a_longitude, "y": cml.site_a_latitude}
    site_b = {"x": cml.site_b_longitude, "y": cml.site_b_latitude}

    distance = float(cml.length.data) * 1000

    # dividing link into 'x'm intervals = segments
    if distance >= segment_size:
        number_of_segments = distance / segment_size
    else:
        number_of_segments = 1

    # calculating gaps between each point in link
    gap_long = (site_b["x"] - site_a["x"]) / np.floor(number_of_segments)
    gap_lat = (site_b["y"] - site_a["y"]) / np.floor(number_of_segments)

    # append into list_of_points series of digits representing number of points bordering segments
    list_of_points = []
    i = 1
    while i <= np.floor(number_of_segments) + 1:
        list_of_points.append(i)
        i += 1
    cml["segment_points"] = list_of_points

    # append coordinates of each point into lat_coords & long_coords
    long_coords = []
    lat_coords = []
    step = 0
    while step <= number_of_segments:
        next_long_point = site_a["x"] + gap_long * step
        next_lat_point = site_a["y"] + gap_lat * step

        long_coords.append(next_long_point)
        lat_coords.append(next_lat_point)
        step += 1

    cml_data_id = []
    rr = 1
    while rr <= len(list_of_points):
        cml_data_id.append(int(cml.cml_id.data))
        rr += 1

    cml["long_array"] = ("segment_points", long_coords)
    cml["lat_array"] = ("segment_points", lat_coords)
    cml["cml_reference"] = ("segment_points", cml_data_id) # reference to the same CML = use own rain values


def intersection_algorithm(calc_data: list[xr.Dataset], intersections: dict):
    """
    Divide the CMLs into segments based on intersections with other CMLs.

    Original author: Radek Vomočil
    Source: https://github.com/radekvomocil/Telcorain-GIT/blob/master/procedures/links_to_segments.py

    :param calc_data: List of CML datasets to be processed.
    :param intersections: Dictionary of intersections between CMLs.
    """
    # TODO: Refactor this function to make it more readable and maintainable
    def append_operation(
            long_intersections,
            coordinates,
            lat_intersections,
            coordinates_1,
            number_of_intersections,
            num,
            references,
            cml_data
    ):
        long_intersections.append(coordinates)
        lat_intersections.append(coordinates_1)
        number_of_intersections.append(num)
        references.append(cml_data)

    def nested_cycle_operation(isecDic, calc_data_op, rain_values_side, side_coords):
        for q in range(len(isecDic)):
            for w in range(len(list(isecDic.values())[q])):
                if list(isecDic.values())[q][w] == side_coords:
                    for z in range(len(calc_data_op)):
                        if (
                            list(isecDic.values())[q][0][0] == calc_data_op[z].site_a_longitude.data
                            and list(isecDic.values())[q][0][1] == calc_data_op[z].site_a_latitude.data
                            and list(isecDic.values())[q][-1][0] == calc_data_op[z].site_b_longitude.data
                            and list(isecDic.values())[q][-1][1] == calc_data_op[z].site_b_latitude.data
                        ):
                            rain_values_side.append(float(calc_data_op[z].R.mean().data))
                            break
                        else:
                            continue
                else:
                    continue

    # list into which distances of each intersection of one link will be saved
    distances = []

    for o in range(len(list(intersections.values())[0]) - 1):
        distance = math.dist(list(intersections.values())[0][o], list(intersections.values())[0][o + 1])
        distances.append(distance)

    # list into which coordinates of the longest lines of links will be saved,
    # more precisely the beginning and the end coordinates of the longest line of the link
    CoordsOfLongestLinesOfLinks = []

    for r in range(len(intersections)):
        find_number_of_intersections = []
        segment_points_intersections = []
        long_coords_intersections = []
        lat_coords_intersections = []
        cml_references = []
        number = 1  # Start numbering segments from 1

        # calculate distances for the current set of intersections
        distances = []
        for oo in range(len(list(intersections.values())[r]) - 1):
            distance = math.dist(list(intersections.values())[r][oo], list(intersections.values())[r][oo + 1])
            distances.append(distance)

        largestLine = max(distances)
        for j in range(len(distances)):
            if largestLine == distances[j]:
                CoordsOfLongestLinesOfLinks.append(
                    (
                        list(intersections.values())[r][j],
                        list(intersections.values())[r][j + 1],
                    )
                )

                rain_values_for_longest_path_first_side = []
                rain_values_for_longest_path_second_side = []

                nested_cycle_operation(
                    intersections,
                    calc_data,
                    rain_values_for_longest_path_first_side,
                    list(intersections.values())[r][j],
                )
                lowestRainValueForLongestPathFirstSide = min(rain_values_for_longest_path_first_side)

                nested_cycle_operation(
                    intersections,
                    calc_data,
                    rain_values_for_longest_path_second_side,
                    list(intersections.values())[r][j + 1],
                )
                lowestRainValueForLongestPathSecondSide = min(rain_values_for_longest_path_second_side)

                if (
                        len(rain_values_for_longest_path_first_side) == 1
                        or len(rain_values_for_longest_path_second_side) == 1
                ):
                    middlepart_long = []
                    halfOfLongestLongitude = (
                        list(intersections.values())[r][j][0] + list(intersections.values())[r][j + 1][0]
                    ) / 2
                    halfOfLongestLatitude = (
                        list(intersections.values())[r][j][1] + list(intersections.values())[r][j + 1][1]
                    ) / 2
                    middlepart_long.append((halfOfLongestLongitude, halfOfLongestLatitude))

                    c = None
                    for c in range(len(calc_data)):
                        if lowestRainValueForLongestPathFirstSide == float(calc_data[c].R.mean().data):
                            if list(intersections.values())[r][j][0] in long_coords_intersections:
                                break
                            else:
                                append_operation(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[c].cml_id.data),
                                )
                                break
                        else:
                            continue

                    append_operation(
                        long_coords_intersections,
                        halfOfLongestLongitude,
                        lat_coords_intersections,
                        halfOfLongestLatitude,
                        find_number_of_intersections,
                        number,
                        cml_references,
                        int(calc_data[c].cml_id.data),
                    )

                    for b in range(len(calc_data)):
                        if lowestRainValueForLongestPathSecondSide == float(calc_data[b].R.mean().data):
                            append_operation(
                                long_coords_intersections,
                                list(intersections.values())[r][j + 1][0],
                                lat_coords_intersections,
                                list(intersections.values())[r][j + 1][1],
                                find_number_of_intersections,
                                number,
                                cml_references,
                                int(calc_data[b].cml_id.data),
                            )
                            break
                        else:
                            continue
                else:
                    # calculate three parts for the longest path
                    firstThirdLongitude = (
                        2 * list(intersections.values())[r][j][0] + list(intersections.values())[r][j + 1][0]
                    ) / 3
                    firstThirdLatitude = (
                        2 * list(intersections.values())[r][j][1] + list(intersections.values())[r][j + 1][1]
                    ) / 3
                    secondThirdLongitude = (
                        list(intersections.values())[r][j][0] + 2 * list(intersections.values())[r][j + 1][0]
                    ) / 3
                    secondThirdLatitude = (
                        list(intersections.values())[r][j][1] + 2 * list(intersections.values())[r][j + 1][1]
                    ) / 3

                    for v in range(len(calc_data)):
                        if lowestRainValueForLongestPathFirstSide == float(calc_data[v].R.mean().data):
                            if list(intersections.values())[r][j][0] in long_coords_intersections:
                                break
                            else:
                                append_operation(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[v].cml_id.data),
                                )
                                break
                        else:
                            continue

                    append_operation(
                        long_coords_intersections,
                        firstThirdLongitude,
                        lat_coords_intersections,
                        firstThirdLatitude,
                        find_number_of_intersections,
                        number,
                        cml_references,
                        int(calc_data[v].cml_id.data),
                    )

                    append_operation(
                        long_coords_intersections,
                        secondThirdLongitude,
                        lat_coords_intersections,
                        secondThirdLatitude,
                        find_number_of_intersections,
                        number,
                        cml_references,
                        int(calc_data[v].cml_id.data),
                    )

                    for n in range(len(calc_data)):
                        if lowestRainValueForLongestPathSecondSide == float(calc_data[n].R.mean().data):
                            append_operation(
                                long_coords_intersections,
                                list(intersections.values())[r][j + 1][0],
                                lat_coords_intersections,
                                list(intersections.values())[r][j + 1][1],
                                find_number_of_intersections,
                                number,
                                cml_references,
                                int(calc_data[n].cml_id.data),
                            )
                            break
                        else:
                            continue
            else:
                rain_values_for_shorter_path_first_side = []
                rain_values_for_shorter_path_second_side = []

                nested_cycle_operation(
                    intersections,
                    calc_data,
                    rain_values_for_shorter_path_first_side,
                    list(intersections.values())[r][j],
                )
                lowestRainValueForShorterPathFirstSide = min(rain_values_for_shorter_path_first_side)

                nested_cycle_operation(
                    intersections,
                    calc_data,
                    rain_values_for_shorter_path_second_side,
                    list(intersections.values())[r][j + 1],
                )
                lowestRainValueForShorterPathSecondSide = min(rain_values_for_shorter_path_second_side)

                if (
                        len(rain_values_for_shorter_path_first_side) == 1
                        or len(rain_values_for_shorter_path_second_side) == 1
                ):
                    lowestRainValue = min(
                        lowestRainValueForShorterPathFirstSide, lowestRainValueForShorterPathSecondSide
                    )
                    for m in range(len(calc_data)):
                        if lowestRainValue == float(calc_data[m].R.mean().data):
                            if list(intersections.values())[r][j][0] in long_coords_intersections:
                                append_operation(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j + 1][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j + 1][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[m].cml_id.data),
                                )
                                break
                            else:
                                append_operation(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[m].cml_id.data),
                                )
                                append_operation(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j + 1][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j + 1][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[m].cml_id.data),
                                )
                                break
                else:
                    # middle point for the shorter path
                    halfOfShorterPathLongitude = (
                        list(intersections.values())[r][j][0] + list(intersections.values())[r][j + 1][0]
                    ) / 2
                    halfOfShorterPathLatitude = (
                        list(intersections.values())[r][j][1] + list(intersections.values())[r][j + 1][1]
                    ) / 2

                    qq = None
                    for qq in range(len(calc_data)):
                        if lowestRainValueForShorterPathFirstSide == float(calc_data[qq].R.mean().data):
                            if list(intersections.values())[r][j][0] in long_coords_intersections:
                                break
                            else:
                                append_operation(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[qq].cml_id.data),
                                )
                                break
                        else:
                            continue

                    append_operation(
                        long_coords_intersections,
                        halfOfShorterPathLongitude,
                        lat_coords_intersections,
                        halfOfShorterPathLatitude,
                        find_number_of_intersections,
                        number,
                        cml_references,
                        int(calc_data[qq].cml_id.data),
                    )

                    for ww in range(len(calc_data)):
                        if lowestRainValueForShorterPathSecondSide == float(calc_data[ww].R.mean().data):
                            append_operation(
                                long_coords_intersections,
                                list(intersections.values())[r][j + 1][0],
                                lat_coords_intersections,
                                list(intersections.values())[r][j + 1][1],
                                find_number_of_intersections,
                                number,
                                cml_references,
                                int(calc_data[ww].cml_id.data),
                            )
                            break
                        else:
                            continue

        # assign segment numbers
        sections = 1
        while sections <= len(find_number_of_intersections):
            segment_points_intersections.append(sections)
            sections += 1

        # verify that all arrays have the same length
        lengths = [
            len(segment_points_intersections),
            len(long_coords_intersections),
            len(lat_coords_intersections),
            len(cml_references),
        ]
        if len(set(lengths)) != 1:
            raise ValueError(
                f"Inconsistent array lengths: {lengths}. All arrays must have the same length."
            )

        for spoj in range(len(calc_data)):
            if (
                list(intersections.values())[r][0][0] == calc_data[spoj].site_a_longitude.data
                and list(intersections.values())[r][0][1] == calc_data[spoj].site_a_latitude.data
                and list(intersections.values())[r][-1][0] == calc_data[spoj].site_b_longitude.data
                and list(intersections.values())[r][-1][1] == calc_data[spoj].site_b_latitude.data
            ):
                # assign data to the current_cml dataset
                calc_data[spoj]["segment_points"] = ("segment_points", segment_points_intersections)
                calc_data[spoj]["long_array"] = ("segment_points", long_coords_intersections)
                calc_data[spoj]["lat_array"] = ("segment_points", lat_coords_intersections)
                calc_data[spoj]["cml_reference"] = ("segment_points", cml_references)
