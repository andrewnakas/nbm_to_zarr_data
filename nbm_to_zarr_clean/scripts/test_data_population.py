#!/usr/bin/env python3
"""Test that data population works correctly."""

import sys
from pathlib import Path

import pandas as pd

from nbm_to_zarr.noaa.nbm_conus.forecast import NbmConusForecastDataset


def test_data_population() -> bool:
    """Test data population from a single GRIB file."""
    print("=" * 60)
    print("Testing NBM data population")
    print("=" * 60)

    try:
        # Create dataset
        dataset = NbmConusForecastDataset()

        # Run operational update
        output_dir = Path("./test_data")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Clean up previous test
        output_path = output_dir / "noaa-nbm-conus-forecast.zarr"
        if output_path.exists():
            import shutil
            shutil.rmtree(output_path)

        dataset.operational_update(output_dir)

        print(f"\n{'='*60}")
        print("Verifying data population...")
        print(f"{'='*60}\n")

        # Open and check the result
        import xarray as xr

        ds = xr.open_zarr(output_path, consolidated=True)

        print(f"Dataset dimensions: {dict(ds.dims)}")
        print(f"\nData variable population:\n")

        total_populated = 0
        for var in ds.data_vars:
            non_nan_count = (~ds[var].isnull()).sum().values
            total_count = ds[var].size
            pct = (non_nan_count / total_count) * 100

            if non_nan_count > 0:
                total_populated += 1

            status = "✅" if non_nan_count > 0 else "❌"
            print(f"{status} {var}: {non_nan_count}/{total_count} ({pct:.1f}% populated)")

        ds.close()

        print(f"\n{'='*60}")
        if total_populated > 0:
            print(f"✅ SUCCESS: {total_populated}/{len(ds.data_vars)} variables have data!")
            print(f"{'='*60}\n")
            return True
        else:
            print(f"❌ FAILED: No variables have data")
            print(f"{'='*60}\n")
            return False

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_data_population()
    sys.exit(0 if success else 1)
