#!/usr/bin/env python3
"""
 @author Johannes Aalto
 SPDX-License-Identifier: Apache-2.0
"""

from datetime import datetime
import os
import warnings

import pandas as pd
import requests

from typing import Iterable, Dict, List
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)

try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=".env")
except ImportError:
    pass


def get_tag_cols(dataframe_keys: Iterable) -> Iterable:
    """Filter out dataframe keys that are not tags"""
    return (
        k
        for k in dataframe_keys
        if not k.startswith("_") and k not in ["result", "table"]
    )


def get_influxdb_lines(df: pd.DataFrame) -> str:
    """
    Convert the Pandas Dataframe into InfluxDB line protocol.

    The dataframe should be similar to results received from query_api.query_data_frame()

    Not quite sure if this supports all kinds if InfluxDB schemas.
    It might be that influxdb_client package could be used as an alternative to this,
    but I'm not sure about the authorizations and such.

    Protocol description: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
    """
    line = df["_measurement"]

    for col_name in get_tag_cols(df):
        line += ("," + col_name + "=") + df[col_name].astype(str)

    line += (
        " "
        + df["_field"]
        + "="
        + df["_value"].astype(str)
        + " "
        + df["_time"].astype(int).astype(str)
    )
    return "\n".join(line)


def main(args: Dict[str, str]):
    print("args: " + str(args.keys()))
    bucket = args.pop("bucket")
    url = args.pop("vm_addr")
    batch_size = args.pop("batch_size")

    if args['start_time'] is not None:
        start = int(datetime.fromisoformat(args.pop('start_time')).timestamp())
    else:
        start = '0'

    if args['stop_time'] is not None:
        stop = int(datetime.fromisoformat(args.pop('stop_time')).timestamp())
    else:
        stop = 'now()'

    for k, v in args.items():
        if v is not None:
            os.environ[k] = v
        print(f"Using {k}={os.getenv(k)}")

    client = InfluxDBClient.from_env_properties()

    query_api = client.query_api()  # use synchronous to see errors

    # Get all unique series by reading first entry of every table.
    # With latest InfluxDB we could possibly use "schema.measurements()" but this doesn't exist in 2.0
    #first_in_series = f"""
    #from(bucket: "{bucket}")
    #|> range(start: {start}, stop: {stop})
    #|> first()
    #"""
    measurement_query = f"""
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "{bucket}")
    """
    print(measurement_query)
    measurements: List[pd.DataFrame] = query_api.query_data_frame(measurement_query)["_value"]
    print(f"Found {len(measurements)} different measurements")
    for measurement in measurements:
        field_query = f"""
        import "influxdata/influxdb/schema"
        schema.measurementFieldKeys(bucket: "{bucket}", measurement: "{measurement}", start: {start})
        """
        fields: List[pd.DataFrame] = query_api.query_data_frame(field_query)["_value"]
        for field in fields:
            # get all unique measurement-field pairs and then fetch and export them one-by-one.
            # With really large databases the results should be possibly split further
            # Something like query_data_frame_stream() might be then useful.
            print(f"Exporting {measurement}_{field}")
            whole_series = f"""
            from(bucket: "{bucket}")
            |> range(start: {start}, stop: {stop})
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["_field"] == "{field}")
            """
            print(whole_series)
            df = query_api.query_data_frame(whole_series)

            lines = get_influxdb_lines(df)
            # "db" is added as an extra tag for the value.
            for i in range(0, len(lines), batch_size):
                response = requests.post(f"{url}/write?db={bucket}", data=lines[i:i+batch_size])
                if response.status_code in [200, 204]:
                    continue
                print("Error while writing to Victoria Metrics")
                print(response.text)
                return


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Script for exporting InfluxDB data into victoria metrics instance. \n"
        " InfluxDB settings can be defined on command line or as environment variables"
        " (or in .env file if python-dotenv is installed)."
        " InfluxDB related args described in \n"
        "https://github.com/influxdata/influxdb-client-python#via-environment-properties"
    )
    parser.add_argument(
        "bucket",
        type=str,
        help="InfluxDB source bucket",
    )
    parser.add_argument(
        "--INFLUXDB_V2_ORG",
        "-o",
        type=str,
        help="InfluxDB organization",
    )
    parser.add_argument(
        "--INFLUXDB_V2_URL",
        "-u",
        type=str,
        help="InfluxDB Server URL, e.g., http://localhost:8086",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TOKEN",
        "-t",
        type=str,
        help="InfluxDB access token.",
    )
    parser.add_argument(
        "--INFLUXDB_V2_SSL_CA_CERT",
        "-S",
        type=str,
        help="Server SSL Cert",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TIMEOUT",
        "-T",
        type=str,
        help="InfluxDB timeout",
    )
    parser.add_argument(
        "--INFLUXDB_V2_VERIFY_SSL",
        "-V",
        type=str,
        help="Verify SSL CERT.",
    )

    parser.add_argument(
        "--vm-addr",
        "-a",
        type=str,
        help="VictoriaMetrics server",
    )

    parser.add_argument(
        "--stop-time",
        type=str,
        help="the stop time of data to extract in ISO format, e.g., '2024-12-15T00:00:00"
    )

    parser.add_argument(
        "--start-time",
        type=str,
        help="the start time of data to extract in ISO format, e.g., '2024-12-14T20:00:00"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000
        help="the maximum number of lines to send to victoria metrics at once"
    )
    main(vars(parser.parse_args()))
