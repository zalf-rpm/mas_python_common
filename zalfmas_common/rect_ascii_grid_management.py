# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
# Susanne Schulz <susanne.schulz@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import gzip
import json

import numpy as np
from pyproj import CRS, Transformer
from scipy.interpolate import NearestNDInterpolator


def read_header(path_to_ascii_grid_file, no_of_header_lines=6):
    """read metadata from esri ascii grid file"""

    def read_header_from(f):
        possible_headers = [
            "ncols",
            "nrows",
            "xllcorner",
            "yllcorner",
            "cellsize",
            "nodata_value",
        ]
        metadata = {}
        header_str = ""
        for i in range(0, no_of_header_lines):
            line = f.readline()
            s_line = [x for x in line.split() if len(x) > 0]
            key = s_line[0].strip().lower()
            if len(s_line) > 1 and key in possible_headers:
                metadata[key] = float(s_line[1].strip())
                header_str += line
        return metadata, header_str

    if path_to_ascii_grid_file[-3:] == ".gz":
        with gzip.open(path_to_ascii_grid_file, mode="rt") as _:
            return read_header_from(_)

    with open(path_to_ascii_grid_file) as _:
        return read_header_from(_)


def create_interpolator_from_rect_grid(
    grid,
    metadata,
    ignore_nodata=True,
    transform_func=None,
    row_col_value=False,
    no_points_to_values=False,
):
    """Create an interpolator from the given grid.
    It is assumed that the values in the grid have a rectangular projection
    so the interpolators underlying distance calculations make sense.
    grid - 2D (numpy) array of values
    metadata - data describing the grid
    transform_func - a function f(r, h) -> (r, h) to transform the r, h values before storing them"""

    rows, cols = grid.shape

    cellsize = int(metadata["cellsize"])
    xll = int(metadata["xllcorner"])
    yll = int(metadata["yllcorner"])
    nodata_value = metadata["nodata_value"]

    xll_center = xll + cellsize // 2
    yll_center = yll + cellsize // 2
    yul_center = yll_center + (rows - 1) * cellsize

    points = []
    values = []
    points_to_values = {}

    for row in range(rows):
        for col in range(cols):
            value = grid[row, col]
            if ignore_nodata and value == nodata_value:
                continue
            r = xll_center + col * cellsize
            h = yul_center - row * cellsize

            if transform_func:
                r, h = transform_func(r, h)

            points.append([r, h])
            values.append((row, col, value) if row_col_value else value)

            if not no_points_to_values:
                points_to_values[(r, h)] = value

    return NearestNDInterpolator(
        np.array(points), np.array(values)
    ), None if no_points_to_values else points_to_values


def interpolate_from_latlon(interpolator, interpolator_crs):
    input_crs = CRS.from_epsg(4326)
    transformer = Transformer.from_crs(input_crs, interpolator_crs, always_xy=True)

    def interpol(lat, lon):
        r, h = transformer.transform(lon, lat)
        return interpolator(r, h)

    return interpol


def rect_coordinates_to_latlon(rect_crs, coords):
    latlon_crs = CRS.from_epsg(4326)
    transformer = Transformer.from_crs(rect_crs, latlon_crs, always_xy=True)

    rs, hs = zip(*coords)
    lons, lats = transformer.transform(list(rs), list(hs))
    latlons = list(zip(lats, lons))

    return latlons


def create_interpolator_from_ascii_grid(
    path_to_ascii_grid, datatype=int, no_of_header_rows=6, ignore_nodata=True
):
    grid, metadata = load_grid_and_metadata_from_ascii_grid(
        path_to_ascii_grid, datatype, no_of_header_rows
    )
    return create_interpolator_from_rect_grid(grid, metadata, ignore_nodata)


def load_grid_and_metadata_from_ascii_grid(
    path_to_ascii_grid, datatype=int, no_of_header_rows=6
):
    metadata, _ = read_header(path_to_ascii_grid)
    grid = np.loadtxt(path_to_ascii_grid, dtype=datatype, skiprows=no_of_header_rows)
    return (grid, metadata)


def load_grid_cached(path_to_grid, val_type, print_path=False):
    if not hasattr(load_grid_cached, "cache"):
        load_grid_cached.cache = {}

    if path_to_grid in load_grid_cached.cache:
        return load_grid_cached.cache[path_to_grid]

    md, _ = read_header(path_to_grid)
    grid = np.loadtxt(path_to_grid, dtype=type, skiprows=len(md))
    print("read: ", path_to_grid)
    ll0r = get_lat_0_lon_0_resolution_from_grid_metadata(md)

    def col(lon):
        return int((lon - ll0r["lon_0"]) / ll0r["res"])

    def row(lat):
        return int((ll0r["lat_0"] - lat) / ll0r["res"])

    def value(lat, lon, return_no_data=False):
        c = col(lon)
        r = row(lat)
        if 0 <= r < md["nrows"] and 0 <= c < md["ncols"]:
            val = val_type(grid[r, c])
            if val != md["nodata_value"] or return_no_data:
                return val
        return None

    cache_entry = {
        "metadata": md,
        "grid": grid,
        "ll0r": ll0r,
        "col": lambda lon: col(lon),
        "row": lambda lat: row(lat),
        "value": lambda lat, lon, ret_no_data: value(lat, lon, ret_no_data),
    }
    load_grid_cached.cache[path_to_grid] = cache_entry
    return cache_entry


def create_climate_geoGrid_interpolator_from_json_file(
    path_to_latlon_to_rowcol_file, worldGeodeticSys84, geoTargetGrid, cdict
):
    "create interpolator from json list of lat/lon to row/col mappings"
    with open(path_to_latlon_to_rowcol_file) as _:
        points = []
        values = []

        transformer = Transformer.from_crs(
            worldGeodeticSys84, geoTargetGrid, always_xy=True
        )

        for latlon, rowcol in json.load(_):
            row, col = rowcol
            clat, clon = latlon
            try:
                cr_geoTargetGrid, ch_geoTargetGrid = transformer.transform(clon, clat)
                cdict[(row, col)] = (round(clat, 4), round(clon, 4))
                points.append([cr_geoTargetGrid, ch_geoTargetGrid])
                values.append((row, col))
                # print "row:", row, "col:", col, "clat:", clat, "clon:", clon, "h:", h, "r:", r, "val:", values[i]
            except:
                continue

        return NearestNDInterpolator(np.array(points), np.array(values))


def get_lat_0_lon_0_resolution_from_grid_metadata(metadata):
    lat_0 = (
        float(metadata["yllcorner"])
        + (float(metadata["cellsize"]) * float(metadata["nrows"]))
        - (float(metadata["cellsize"]) / 2.0)
    )
    lon_0 = float(metadata["xllcorner"]) + (float(metadata["cellsize"]) / 2.0)
    resolution = float(metadata["cellsize"])
    return {"lat_0": lat_0, "lon_0": lon_0, "res": resolution}
