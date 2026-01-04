#!/usr/bin/env python3
"""Clean up old forecast data to maintain rolling storage."""

import argparse
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import xarray as xr


def cleanup_old_forecasts(max_age_hours: int = 24, keep_latest_only: bool = True) -> None:
    """Remove forecast data older than the specified age.

    Args:
        max_age_hours: Maximum age of forecasts to keep in hours (ignored if keep_latest_only=True)
        keep_latest_only: If True, keep only the most recent forecast run(s)
    """
    data_dir = Path("data")

    # Process each Zarr dataset
    for zarr_path in data_dir.glob("*.zarr"):
        try:
            # Open the dataset
            ds = xr.open_zarr(zarr_path, consolidated=True)

            # Get the append dimension (usually 'init_time')
            append_dim = "init_time"
            if append_dim not in ds.dims:
                print(f"Warning: {zarr_path} does not have '{append_dim}' dimension")
                ds.close()
                continue

            # Find indices to keep
            init_times = pd.DatetimeIndex(ds[append_dim].values)

            # Ensure timezone-naive for comparison
            if init_times.tz is not None:
                init_times = init_times.tz_localize(None)

            if keep_latest_only:
                # Keep only the most recent forecast run (most recent init_time)
                # This keeps just 1 forecast run with all its lead times
                max_init_time = init_times.max()
                keep_mask = init_times == max_init_time
                print(f"Keeping only latest forecast: {max_init_time}")
            else:
                # Keep forecasts within the time window
                cutoff_time = pd.Timestamp.now(tz="UTC") - timedelta(hours=max_age_hours)
                cutoff_time_naive = cutoff_time.tz_localize(None) if cutoff_time.tz is not None else cutoff_time
                keep_mask = init_times >= cutoff_time_naive
                print(f"Keeping forecasts newer than {cutoff_time_naive}")

            if keep_mask.sum() == 0:
                print(f"Warning: All data in {zarr_path} would be removed - keeping as is")
                ds.close()
                continue

            if keep_mask.all():
                print(f"No cleanup needed for {zarr_path}")
                ds.close()
                continue

            # Select only data to keep
            ds_recent = ds.isel({append_dim: keep_mask})

            # Create a temporary path
            temp_path = zarr_path.parent / f"{zarr_path.name}.tmp"

            # Save the filtered dataset
            ds_recent.to_zarr(temp_path, mode="w", consolidated=True)

            ds.close()
            ds_recent.close()

            # Replace the old dataset
            shutil.rmtree(zarr_path)
            temp_path.rename(zarr_path)

            removed_count = (~keep_mask).sum()
            kept_count = keep_mask.sum()
            print(f"✅ Kept {kept_count} init_time(s), removed {removed_count} from {zarr_path.name}")

        except Exception as e:
            print(f"Error processing {zarr_path}: {e}")
            continue


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Clean up old forecast data")
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=24,
        help="Maximum age of forecasts to keep in hours (default: 24, ignored if --keep-latest-only)",
    )
    parser.add_argument(
        "--keep-latest-only",
        action="store_true",
        default=True,
        help="Keep only the most recent forecast run (default: True)",
    )
    parser.add_argument(
        "--no-keep-latest-only",
        dest="keep_latest_only",
        action="store_false",
        help="Use max-age-hours instead of keeping only latest",
    )

    args = parser.parse_args()
    cleanup_old_forecasts(args.max_age_hours, args.keep_latest_only)


if __name__ == "__main__":
    main()
