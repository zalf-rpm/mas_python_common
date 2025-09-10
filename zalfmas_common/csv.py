#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Susanne Schulz <susanne.schulz@zalf.de>
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv


def read_csv(path_to_setups_csv, key="id", key_type=(int,)):
    """read sim setup from csv file"""
    composite_key = type(key) is tuple
    keys = {i: v for i, v in enumerate(key)} if composite_key else {0: key}
    key_types = {i: v for i, v in enumerate(key_type)}

    with open(path_to_setups_csv) as _:
        key_to_data = {}
        # determine seperator char
        dialect = csv.Sniffer().sniff(_.read(), delimiters=";,\t")
        _.seek(0)
        # read csv with seperator char
        reader = csv.reader(_, dialect)
        header_cols = next(reader)

        for row in reader:
            data = {}
            for i, header_col in enumerate(header_cols):
                value = row[i]
                if value.lower() in ["true", "false"]:
                    value = value.lower() == "true"
                if composite_key and header_col in keys.values():
                    for i, k in keys.items():
                        if header_col == k:
                            value = key_types.get(i, key_types[0])(value)
                            break
                elif header_col == key:
                    value = key_types[0](value)
                data[header_col] = value
            if composite_key:
                key_vals = tuple(
                    [key_types.get(i, key_types[0])(data[k]) for i, k in keys.items()]
                )
            else:
                key_vals = key_types[0](data[key])
            key_to_data[key_vals] = data
        return key_to_data
