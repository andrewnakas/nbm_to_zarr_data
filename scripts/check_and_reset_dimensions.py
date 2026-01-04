#!/usr/bin/env python3
"""Check if Zarr store dimensions match expected and reset if needed."""

import shutil
from pathlib import Path

import xarray as xr

from nbm_to_zarr.noaa.nbm_conus.forecast import NbmConusForecastDataset


def check_and_reset_dimensions() -> None:
    """Check if existing Zarr store has correct dimensions, reset if not."""
    data_dir = Path("data")
    dataset = NbmConusForecastDataset()
    zarr_path = data_dir / f"{dataset.dataset_id}.zarr"

    if not zarr_path.exists():
        print(f"✅ No existing Zarr store at {zarr_path} - will create fresh")
        return

    try:
        # Open existing store
        ds = xr.open_zarr(zarr_path, consolidated=True)

        # Check dimensions
        expected_dims = dataset.template_config.dimensions
        actual_dims = dict(ds.sizes)

        mismatch = False
        for dim, expected_size in expected_dims.items():
            actual_size = actual_dims.get(dim)
            if actual_size is None:
                print(f"⚠️  Missing dimension: {dim}")
                mismatch = True
            elif actual_size != expected_size:
                print(f"⚠️  Dimension mismatch: {dim} (expected: {expected_size}, actual: {actual_size})")
                mismatch = True

        ds.close()

        if mismatch:
            print(f"\n🗑️  Removing old Zarr store with incorrect dimensions...")
            shutil.rmtree(zarr_path)
            print(f"✅ Removed {zarr_path} - will create fresh with correct dimensions")
        else:
            print(f"✅ Zarr store dimensions match expected: {expected_dims}")

    except Exception as e:
        print(f"⚠️  Error checking Zarr store: {e}")
        print(f"🗑️  Removing potentially corrupted Zarr store...")
        shutil.rmtree(zarr_path)
        print(f"✅ Removed {zarr_path} - will create fresh")


if __name__ == "__main__":
    check_and_reset_dimensions()
