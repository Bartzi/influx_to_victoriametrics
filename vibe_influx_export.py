import argparse
import gzip
import warnings

from datetime import datetime, timedelta, timezone
from typing import Iterator

import pandas as pd
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.flux_csv_parser import FluxQueryException
from influxdb_client.rest import ApiException
from influxdb_client.client.warnings import MissingPivotFunction
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


warnings.simplefilter("ignore", MissingPivotFunction)


def str2bool(v: str) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    if v.lower() in ("no", "false", "f", "n", "0"):
        return False
    raise argparse.ArgumentTypeError("Boolean value expected.")


def create_vm_session(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def iter_time_windows(start: datetime, end: datetime, step: timedelta) -> Iterator[tuple[datetime, datetime]]:
    current = start
    while current < end:
        window_end = min(current + step, end)
        yield current, window_end
        current = window_end


def chunked(items: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


FLUX_SYSTEM_COLUMNS = {
    "result",
    "table",
    "_start",
    "_stop",
    "_time",
    "_value",
    "_field",
    "_measurement",
}


def clean_tag_value(val: object) -> str | None:
    """Return a clean string for a tag value, or None if empty/NaN."""
    if val is None or pd.isna(val):
        return None
    # Strip leading/trailing whitespace and flatten any embedded newlines/tabs
    cleaned = " ".join(str(val).split())


def flux_df_to_lines(df: pd.DataFrame) -> tuple[list[str], int]:
    """Convert any Flux DataFrame to Influx Line Protocol strings with dynamic tag discovery."""
    if df.empty:
        return [], 0

    required_cols = {"_measurement", "_time", "_field", "_value"}
    if not required_cols.issubset(df.columns):
        return [], 0

    clean_df = df.dropna(subset=list(required_cols))
    if clean_df.empty:
        return [], 0

    # Dynamically detect all custom tag columns (everything not in Flux system columns)
    tag_cols = [col for col in clean_df.columns if col not in FLUX_SYSTEM_COLUMNS]

    lines: list[str] = []
    skipped_strings = 0

    # itertuples(index=False) on a dynamic column list is fast; map column names to positional indices
    col_names = list(clean_df.columns)
    idx_measurement = col_names.index("_measurement")
    idx_time = col_names.index("_time")
    idx_field = col_names.index("_field")
    idx_value = col_names.index("_value")
    tag_indices = [(col, col_names.index(col)) for col in tag_cols]

    for row in clean_df.itertuples(index=False, name=None):
        raw_val = row[idx_value]

        # Convert booleans to 1/0, keep ints/floats, attempt numeric parse on strings
        if isinstance(raw_val, bool):
            numeric_val: int | float = 1 if raw_val else 0
        elif isinstance(raw_val, (int, float)):
            if pd.isna(raw_val):
                continue
            numeric_val = float(raw_val) if isinstance(raw_val, float) else int(raw_val)
        else:
            val_str = str(raw_val).strip()
            if val_str.lower() in ("true", "false"):
                numeric_val = 1 if val_str.lower() == "true" else 0
            else:
                try:
                    numeric_val = float(val_str)
                except ValueError:
                    skipped_strings += 1
                    continue

        measurement = " ".join(str(row[idx_measurement]).split())
        field_name = " ".join(str(row[idx_field]).split())
        if not measurement or not field_name:
            continue

        p = Point(measurement).time(row[idx_time], WritePrecision.MS)

        # Dynamically clean and attach every discovered tag column
        for tag_name, tag_idx in tag_indices:
            clean_val = clean_tag_value(row[tag_idx])
            if clean_val is not None:
                clean_key = " ".join(str(tag_name).split())
                p.tag(clean_key, clean_val)

        p.field(field_name, numeric_val)
        lines.append(p.to_line_protocol())

    return lines, skipped_strings


def write_batch_to_vm(
    session: requests.Session,
    vm_url: str,
    lines: list[str],
    use_gzip: bool = True,
    timeout: float = 15.0,
) -> bool:
    if not lines:
        return True

    raw_payload = ("\n".join(lines) + "\n").encode("utf-8")
    headers = {"Content-Type": "text/plain; charset=utf-8"}

    if use_gzip:
        payload = gzip.compress(raw_payload)
        headers["Content-Encoding"] = "gzip"
    else:
        payload = raw_payload

    try:
        resp = session.post(
            f"{vm_url.rstrip('/')}/write",
            params={"precision": "ms"},
            data=payload,
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code in (200, 204):
            return True

        print(f"  [VM Error {resp.status_code}] Body: {resp.text!r}")
        return False

    except requests.exceptions.RequestException as exc:
        print(f"  [Request Exception] {exc}")
        return False


def list_measurements_in_window(
    query_api, bucket: str, start_iso: str, stop_iso: str
) -> list[str]:
    """Fetch all _measurement names active in the given bucket and time range."""
    flux_schema = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(
            bucket: "{bucket}",
            start: {start_iso},
            stop: {stop_iso}
        )
    '''
    tables = query_api.query(flux_schema)
    measurements = []
    for table in tables:
        for record in table.records:
            val = record.get_value()
            if val:
                measurements.append(str(val))
    return sorted(set(measurements))


def query_single_measurement_with_split(
    query_api,
    bucket: str,
    measurement: str,
    win_start: datetime,
    win_end: datetime,
    min_step: timedelta = timedelta(minutes=15),
) -> list[pd.DataFrame]:
    """Query one measurement; if it still panics, split into smaller time slices."""
    start_iso = win_start.isoformat()
    stop_iso = win_end.isoformat()
    safe_meas = measurement.replace("\\", "\\\\").replace('"', '\\"')

    flux_query = f'''
        from(bucket: "{bucket}")
          |> range(start: {start_iso}, stop: {stop_iso})
          |> filter(fn: (r) => r._measurement == "{safe_meas}")
    '''
    try:
        res = query_api.query_data_frame(flux_query)
        if isinstance(res, list):
            return [df for df in res if not df.empty]
        return [res] if not res.empty else []

    except (FluxQueryException, ApiException) as exc:
        duration = win_end - win_start
        if duration <= min_step:
            print(
                f"  [SKIPPED CORRUPT DATA] measurement={measurement!r} | "
                f"{start_iso} -> {stop_iso} | Panic: {exc}"
            )
            return []

        sub_step = timedelta(hours=1) if duration > timedelta(hours=1) else min_step
        print(
            f"  [Splitting Measurement] {measurement!r} panicked; "
            f"retrying in {sub_step} slices..."
        )
        dfs: list[pd.DataFrame] = []
        for sub_start, sub_end in iter_time_windows(win_start, win_end, sub_step):
            dfs.extend(
                query_single_measurement_with_split(
                    query_api, bucket, measurement, sub_start, sub_end, min_step
                )
            )
        return dfs


def query_window_with_fallback(
    query_api,
    bucket: str,
    win_start: datetime,
    win_end: datetime,
    min_step: timedelta = timedelta(minutes=15),
) -> list[pd.DataFrame]:
    """Try fast full-bucket query first; on InfluxDB panic, fall back per measurement."""
    start_iso = win_start.isoformat()
    stop_iso = win_end.isoformat()
    flux_query = f'''
        from(bucket: "{bucket}")
          |> range(start: {start_iso}, stop: {stop_iso})
    '''
    try:
        res = query_api.query_data_frame(flux_query)
        if isinstance(res, list):
            return [df for df in res if not df.empty]
        return [res] if not res.empty else []

    except FluxQueryException as exc:
        print(f"  [InfluxDB Panic on Full Bucket] {exc}")
        print(f"  Falling back to per-measurement export for {start_iso} -> {stop_iso}...")

        try:
            measurements = list_measurements_in_window(query_api, bucket, start_iso, stop_iso)
        except FluxQueryException:
            # Fallback if schema.measurements itself panics on that time range
            measurements = list_measurements_in_window(
                query_api, bucket, "1970-01-01T00:00:00Z", "now()"
            )

        print(f"  Found {len(measurements)} measurements to query individually.")
        dfs: list[pd.DataFrame] = []
        for meas in measurements:
            dfs.extend(
                query_single_measurement_with_split(
                    query_api, bucket, meas, win_start, win_end, min_step
                )
            )
        return dfs


def main():
    parser = argparse.ArgumentParser(description="Export InfluxDB v2 data to VictoriaMetrics.")
    parser.add_argument("-o", "--org", required=True, help="InfluxDB organization")
    parser.add_argument("-u", "--url", required=True, help="InfluxDB URL")
    parser.add_argument("-t", "--token", required=True, help="InfluxDB API token")
    parser.add_argument("-b", "--bucket", required=True, help="InfluxDB bucket name")
    parser.add_argument("-V", "--verify-ssl", type=str2bool, default=True, help="Verify SSL certs")
    parser.add_argument("-a", "--vm-address", required=True, help="VictoriaMetrics base URL")
    parser.add_argument("--start-time", required=True, help="ISO start time (e.g. 2024-11-28T00:00:00+00:00)")
    parser.add_argument("--end-time", default=None, help="ISO end time (defaults to now UTC)")
    parser.add_argument("--window-hours", type=int, default=24, help="Pagination window in hours (default: 24)")
    parser.add_argument("--batch-size", type=int, default=5000, help="Lines per HTTP POST (default: 5000)")
    args = parser.parse_args()

    start_dt = datetime.fromisoformat(args.start_time)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    end_dt = datetime.fromisoformat(args.end_time) if args.end_time else datetime.now(timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    step = timedelta(hours=args.window_hours)

    total_written = 0
    total_skipped_strings = 0

    with (
        InfluxDBClient(
            url=args.url,
            token=args.token,
            org=args.org,
            verify_ssl=args.verify_ssl,
            timeout=120_000,  # 120s query timeout per window
        ) as influx_client,
        create_vm_session() as vm_session,
    ):
        query_api = influx_client.query_api()

        for win_start, win_end in iter_time_windows(start_dt, end_dt, step):
            start_iso = win_start.isoformat()
            stop_iso = win_end.isoformat()
            print(f"Querying window {start_iso} -> {stop_iso} ...")

            result = query_window_with_fallback(query_api, args.bucket, win_start, win_end)

            # InfluxDB returns either a single DataFrame or a list of DataFrames
            if not result:
                print("  No data in window.")
                continue
            if isinstance(result, list):
                df = pd.concat(result, ignore_index=True)
            else:
                df = result

            if df.empty:
                print("  No data in window.")
                continue

            lines, skipped = flux_df_to_lines(df)
            total_skipped_strings += skipped

            if not lines:
                print(f"  0 numeric lines to write (skipped {skipped} string rows).")
                continue

            window_written = 0
            for batch in chunked(lines, args.batch_size):
                if write_batch_to_vm(vm_session, args.vm_address, batch, use_gzip=True):
                    window_written += len(batch)
                else:
                    print(f"  FAILED writing batch in window {start_iso} -> {stop_iso}. Stopping.")
                    return

            total_written += window_written
            print(f"  Wrote {window_written:,} points (skipped {skipped} string rows).")

    print(f"\nDone! Total points written: {total_written:,} | Total string rows skipped: {total_skipped_strings:,}")


if __name__ == "__main__":
    main()