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
    Divide the CMLs into segments based on intersections with other CMLs, and assing the CML reference to each segment
    point with the priority of lower rain rates during the comparison of the intersecting segments.

    Original author: Radek Vomočil
    Source: https://github.com/radekvomocil/Telcorain-GIT/blob/master/procedures/links_to_segments.py

    :param calc_data: List of CML datasets to be processed.
    :param intersections: Dictionary of intersections between CMLs.
    """
    # TODO: Refactor (simplify) this function to make it more readable and maintainable, add missing type hints
    #  (currently refactored using mostly "heavy force" and ChatGPT)
    """
    Theoretical Algorithm Proposal:
    
    I. Calculation of path-averaged rain rates:  
       Path-averaged rain rates are calculated for individual CMLs using the standard approach.
    
    II. Individual processing of each CML in a cycle:
    
        1. Division of the CML Path into Segments:  
           Each CML's path is divided into segments based on intersections with other CMLs,
           ensuring that each segment is bounded either by intersections or by the endpoints of the path.
        
        2. Selection of the Longest Path Segment:  
           The longest path segment of the current CML is identified. The following cases are considered:  
            CASE A. [The segment is bounded by two intersections.]  
                - Lower-level segment parts are determined. The original path segment is divided into three lower-level
                  segments, with the middle part being one-third of the original segment's length.  
                - The rain rate calculated for the current CML (from Step I) is assigned to this middle segment.  
        
            CASE B. [The segment is bounded by an intersection and a path endpoint.]  
                - Lower-level segment parts are determined. The original path
                  segment is divided into two equal-length segments (halves).  
                - The segment adjacent to the path endpoint is assigned the
                  rain rate calculated for the current CML (from Step I).  
        
        3. Assignment of Rain Rates at Intersection Points:  
           At each intersection point, the rain rates of the two intersecting CMLs are compared, and the lower rain
           rate is selected. This selected rain rate is applied to the lower-level segment parts adjacent to the
           intersection point. The following cases are considered:  
            CASE A. [The adjacent path segment is the longest segment. (The same segment identified as in Step II.2.)]
                case a. [The segment is bounded by the current and another intersection]
                    - The selected rain rate is applied to the adjacent lower-level segment part,
                      which is one-third of the length of the adjacent path segment.  
                case b. [The segment is bounded by the current intersection and a path endpoint]
                    - The selected rain rate is applied to the adjacent lower-level segment part,
                      which is half the length of the adjacent path segment.  
        
            CASE B. [The adjacent path segment is not the longest segment.]  
                case a. [The segment is bounded by the current and another intersection]
                    - The selected rain rate is applied to the adjacent lower-level segment part,
                      which is half the length of the adjacent path segment.  
                case b. [The segment is bounded by the current intersection and a path endpoint]
                    - The selected rain rate is applied to the entire length of the adjacent path segment.  
        
        4. Continue Processing:  
           Proceed to process the next CML in the cycle.
    """

    # -------------------------------------------------------------------------
    # 1. HELPER FUNCTIONS
    # -------------------------------------------------------------------------
    def cml_coords_match(ds: xr.Dataset, coord_first: tuple, coord_last: tuple) -> bool:
        """
        Returns True if dataset `ds` has site_a matching `coord_first` and site_b matching `coord_last`.
        """
        return (
            ds.site_a_longitude.data == coord_first[0]
            and ds.site_a_latitude.data == coord_first[1]
            and ds.site_b_longitude.data == coord_last[0]
            and ds.site_b_latitude.data == coord_last[1]
        )

    def append_point_data(
        long_intersections,
        long_coordinates,
        lat_intersections,
        lat_coordinates,
        number_of_intersections,
        num,
        references,
        cml_data
    ):
        """
        Helper for appending parallel lists that describe segment points.
        """
        long_intersections.append(long_coordinates)
        lat_intersections.append(lat_coordinates)
        number_of_intersections.append(num)
        references.append(cml_data)

    def nested_cycle_operation(isecDic, calc_data_op, rain_values_side, side_coords):
        """
        Helper that searches across all intersections and calc_data for
        any link matching the side_coords, then appends its R.mean() value.
        """
        for q in range(len(isecDic)):
            coords_list = list(isecDic.values())[q]
            for w in range(len(coords_list)):
                if coords_list[w] == side_coords:
                    # If found intersection matches side_coords,
                    # check each dataset in calc_data_op
                    for z in range(len(calc_data_op)):
                        if cml_coords_match(
                            calc_data_op[z],
                            coords_list[0],      # first intersection
                            coords_list[-1]      # last intersection
                        ):
                            rain_values_side.append(float(calc_data_op[z].R.mean().data))
                            break
                    # continue scanning for next q
                else:
                    continue

    def compute_lowest_rain_values(calc_data_op, intersections_dict, r_index, j_index):
        """
        Compute the minimum rain values for each 'side' of a single path by calling nested_cycle_operation.

        Returns:
            (lowest_value_first, count_first_side, lowest_value_second, count_second_side)
        """
        # First side
        rain_values_first_side = []
        nested_cycle_operation(
            intersections_dict,
            calc_data_op,
            rain_values_first_side,
            list(intersections_dict.values())[r_index][j_index]
        )
        lowest_value_first = min(rain_values_first_side)
        count_first_side = len(rain_values_first_side)

        # Second side
        rain_values_second_side = []
        nested_cycle_operation(
            intersections_dict,
            calc_data_op,
            rain_values_second_side,
            list(intersections_dict.values())[r_index][j_index + 1]
        )
        lowest_value_second = min(rain_values_second_side)
        count_second_side = len(rain_values_second_side)

        return lowest_value_first, count_first_side, lowest_value_second, count_second_side

    def find_and_append_cml_reference(
        calc_data_op,
        side_coord,
        lowest_rain_val,
        long_intersections,
        lat_intersections,
        find_num_of_intersections,
        segment_number,
        cml_refs
    ):
        """
        Loop over calc_data_op, find the dataset whose R.mean() matches 'lowest_rain_value',
        and append the side_coord if not already in long_intersections. Break at the first match.
        """
        for idx in range(len(calc_data_op)):
            if lowest_rain_val == float(calc_data_op[idx].R.mean().data):
                # Avoid appending if the longitude is already there
                if side_coord[0] in long_intersections:
                    break
                else:
                    append_point_data(
                        long_intersections,
                        side_coord[0],
                        lat_intersections,
                        side_coord[1],
                        find_num_of_intersections,
                        segment_number,
                        cml_refs,
                        int(calc_data_op[idx].cml_id.data),
                    )
                    break
            else:
                continue

    def compute_midpoint(coord1: tuple[float, float], coord2: tuple[float, float]) -> tuple[float, float]:
        """
        Compute midpoint of two (long, lat) coordinate pairs.
        """
        return (
            (coord1[0] + coord2[0]) / 2,
            (coord1[1] + coord2[1]) / 2
        )

    def append_side_midpoint_side(
        calc_data_op,
        first_side_coord,
        second_side_coord,
        lowest_r_first,
        lowest_r_second,
        long_coord_intersection,
        lat_coord_intersection,
        find_num_of_intersections,
        segment_number,
        cml_refs
    ):
        """
        Apply the pattern:
          1) Calculate midpoint
          2) Find & append the first side
          3) Append a midpoint
          4) Find & append the second side
        """
        # 1) Use the helper for midpoint
        mid_long, mid_lat = compute_midpoint(
            first_side_coord,
            second_side_coord,
        )

        # 2) First side
        find_and_append_cml_reference(
            calc_data_op,
            first_side_coord,
            lowest_r_first,
            long_coord_intersection,
            lat_coord_intersection,
            find_num_of_intersections,
            segment_number,
            cml_refs
        )
        # 3) Midpoint
        append_point_data(
            long_coord_intersection,
            mid_long,
            lat_coord_intersection,
            mid_lat,
            find_num_of_intersections,
            segment_number,
            cml_refs,
            cml_refs[-1] if cml_refs else -1
        )
        # 4) Second side
        find_and_append_cml_reference(
            calc_data_op,
            second_side_coord,
            lowest_r_second,
            long_coord_intersection,
            lat_coord_intersection,
            find_num_of_intersections,
            segment_number,
            cml_refs
        )

    # -------------------------------------------------------------------------
    # 2. MAIN LOGIC
    # -------------------------------------------------------------------------

    # (Optional) Pre-calculate distances for the first link in intersections
    distances_example = []
    if len(intersections) > 0 and len(list(intersections.values())[0]) > 1:
        for o in range(len(list(intersections.values())[0]) - 1):
            distance = math.dist(
                list(intersections.values())[0][o],
                list(intersections.values())[0][o + 1]
            )
            distances_example.append(distance)

    # Store beginning-end coords of the longest lines
    coords_of_longest_lines_of_links = []

    for r in range(len(intersections)):
        find_number_of_intersections = []
        segment_points_intersections = []
        long_coords_intersections = []
        lat_coords_intersections = []
        cml_references = []
        number = 1  # Start numbering segments from 1

        # Calculate distances for the current set of intersections
        distances = []
        for oo in range(len(list(intersections.values())[r]) - 1):
            dist = math.dist(
                list(intersections.values())[r][oo],
                list(intersections.values())[r][oo + 1]
            )
            distances.append(dist)

        largest_line = max(distances) if distances else 0

        for j in range(len(distances)):
            (
                lowest_rain_first,
                count_first,
                lowest_rain_second,
                count_second
            ) = compute_lowest_rain_values(calc_data, intersections, r, j)

            # The "longest" segment logic
            if largest_line == distances[j]:
                coords_of_longest_lines_of_links.append(
                    (
                        list(intersections.values())[r][j],
                        list(intersections.values())[r][j + 1],
                    )
                )
                # If only one dataset on one side => 1 midpoint
                if count_first == 1 or count_second == 1:
                    append_side_midpoint_side(
                        calc_data,
                        list(intersections.values())[r][j],      # first side
                        list(intersections.values())[r][j + 1],  # second side
                        lowest_rain_first,
                        lowest_rain_second,
                        long_coords_intersections,
                        lat_coords_intersections,
                        find_number_of_intersections,
                        number,
                        cml_references
                    )
                else:
                    # Multiple data on both sides => split into thirds
                    first_third_long = (
                        2 * list(intersections.values())[r][j][0]
                        + list(intersections.values())[r][j+1][0]
                    ) / 3
                    first_third_lat = (
                        2 * list(intersections.values())[r][j][1]
                        + list(intersections.values())[r][j+1][1]
                    ) / 3
                    second_third_long = (
                        list(intersections.values())[r][j][0]
                        + 2 * list(intersections.values())[r][j+1][0]
                    ) / 3
                    second_third_lat = (
                        list(intersections.values())[r][j][1]
                        + 2 * list(intersections.values())[r][j+1][1]
                    ) / 3

                    # First side
                    find_and_append_cml_reference(
                        calc_data,
                        list(intersections.values())[r][j],
                        lowest_rain_first,
                        long_coords_intersections,
                        lat_coords_intersections,
                        find_number_of_intersections,
                        number,
                        cml_references
                    )

                    # The two dividing points
                    append_point_data(
                        long_coords_intersections,
                        first_third_long,
                        lat_coords_intersections,
                        first_third_lat,
                        find_number_of_intersections,
                        number,
                        cml_references,
                        cml_references[-1] if cml_references else -1
                    )
                    append_point_data(
                        long_coords_intersections,
                        second_third_long,
                        lat_coords_intersections,
                        second_third_lat,
                        find_number_of_intersections,
                        number,
                        cml_references,
                        cml_references[-1] if cml_references else -1
                    )

                    # Second side
                    find_and_append_cml_reference(
                        calc_data,
                        list(intersections.values())[r][j+1],
                        lowest_rain_second,
                        long_coords_intersections,
                        lat_coords_intersections,
                        find_number_of_intersections,
                        number,
                        cml_references
                    )

            else:
                # The "shorter" segment logic
                if count_first == 1 or count_second == 1:
                    # Just pick the side with the minimal of the two
                    lowest_rain_value = min(lowest_rain_first, lowest_rain_second)
                    for m in range(len(calc_data)):
                        if lowest_rain_value == float(calc_data[m].R.mean().data):
                            # If first side is already appended, append the second
                            if list(intersections.values())[r][j][0] in long_coords_intersections:
                                append_point_data(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j+1][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j+1][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[m].cml_id.data),
                                )
                                break
                            else:
                                # Append both sides
                                append_point_data(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[m].cml_id.data),
                                )
                                append_point_data(
                                    long_coords_intersections,
                                    list(intersections.values())[r][j+1][0],
                                    lat_coords_intersections,
                                    list(intersections.values())[r][j+1][1],
                                    find_number_of_intersections,
                                    number,
                                    cml_references,
                                    int(calc_data[m].cml_id.data),
                                )
                                break
                else:
                    # multiple CMLs => place a midpoint
                    append_side_midpoint_side(
                        calc_data,
                        list(intersections.values())[r][j],      # first side
                        list(intersections.values())[r][j + 1],  # second side
                        lowest_rain_first,
                        lowest_rain_second,
                        long_coords_intersections,
                        lat_coords_intersections,
                        find_number_of_intersections,
                        number,
                        cml_references
                    )

        # Assign segment numbers
        for seg_id in range(1, len(find_number_of_intersections) + 1):
            segment_points_intersections.append(seg_id)

        # Verify that all arrays have the same length
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

        # Store the resulting arrays back into the matching dataset
        for spoj in range(len(calc_data)):
            first_coord = list(intersections.values())[r][0]
            last_coord  = list(intersections.values())[r][-1]
            if cml_coords_match(calc_data[spoj], first_coord, last_coord):
                calc_data[spoj]["segment_points"] = ("segment_points", segment_points_intersections)
                calc_data[spoj]["long_array"] = ("segment_points", long_coords_intersections)
                calc_data[spoj]["lat_array"] = ("segment_points", lat_coords_intersections)
                calc_data[spoj]["cml_reference"] = ("segment_points", cml_references)
