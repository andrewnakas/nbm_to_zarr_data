#!/usr/bin/env python3
"""Basic test to verify template creation works."""

import sys
from pathlib import Path

import pandas as pd

from nbm_to_zarr.noaa.nbm_conus.forecast import NbmConusTemplateConfig


def test_template_creation() -> bool:
    """Test that we can create a basic template."""
    print("=" * 60)
    print("Testing template creation")
    print("=" * 60)

    try:
        # Create template config
        config = NbmConusTemplateConfig()

        print("\n✅ Template config created")
        print(f"   Dataset ID: {config.dataset_attributes.id}")
        print(f"   Dimensions: {config.dimensions}")
        print(f"   Variables: {len(config.data_vars)}")

        # Try to create a minimal template
        print("\nCreating minimal template...")
        start_time = pd.Timestamp.now(tz="UTC").floor("h")

        ds = config.get_template(
            append_dim_start=start_time,
            append_dim_periods=1,  # Just 1 time step
            append_dim_freq="1h",
        )

        print(f"✅ Template created successfully")
        print(f"   Dimensions: {dict(ds.dims)}")
        print(f"   Variables: {list(ds.data_vars.keys())[:5]}... ({len(ds.data_vars)} total)")
        print(f"   Coordinates: {list(ds.coords.keys())}")

        # Check data types
        print("\nChecking data types...")
        for var in list(ds.data_vars.keys())[:3]:
            print(f"   {var}: {ds[var].dtype}, shape={ds[var].shape}")

        # Try to save to Zarr
        print("\nTrying to save to Zarr...")
        output_path = Path("./test_data/test_template.zarr")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            import shutil

            shutil.rmtree(output_path)

        # Remove timezone from datetime coordinates (Zarr doesn't support timezones)
        print("\nChecking for timezones in coordinates...")
        for coord_name in ds.coords:
            dtype_str = str(ds[coord_name].dtype)
            print(f"  {coord_name}: dtype={dtype_str}")

            # Check if coordinate is datetime-like
            if 'datetime64' in dtype_str:
                print(f"    -> Is datetime type")

                # Check if timezone is in the dtype string (e.g., "datetime64[ns, UTC]")
                if 'UTC' in dtype_str or 'utc' in dtype_str.lower():
                    print(f"    -> Has UTC timezone in dtype string")
                    # Convert values to timezone-naive
                    values = ds[coord_name].values.astype('datetime64[ns]')
                    ds = ds.assign_coords({coord_name: values})
                    print(f"    ✅ Removed timezone from {coord_name}")
                else:
                    # Try pandas approach for other timezone formats
                    dt_index = pd.DatetimeIndex(ds[coord_name].values)
                    print(f"    -> dt_index.tz = {dt_index.tz}")
                    if dt_index.tz is not None:
                        values = dt_index.tz_localize(None).to_numpy()
                        ds = ds.assign_coords({coord_name: values})
                        print(f"    ✅ Removed timezone from {coord_name}")

        ds.to_zarr(output_path, mode="w", consolidated=True, zarr_version=2)

        print(f"✅ Successfully saved to {output_path}")
        print(f"   Size: {sum(f.stat().st_size for f in output_path.rglob('*') if f.is_file()) / 1024:.1f} KB")

        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_template_creation()
    sys.exit(0 if success else 1)
