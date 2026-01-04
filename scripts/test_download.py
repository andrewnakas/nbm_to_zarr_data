#!/usr/bin/env python3
"""Test script for downloading and processing a minimal NBM dataset."""

import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

from nbm_to_zarr.base.region_job import ProcessingRegion
from nbm_to_zarr.noaa.nbm_conus.forecast import (
    NbmConusForecastRegionJob,
    NbmConusTemplateConfig,
)


def test_minimal_download() -> bool:
    """Test downloading a minimal subset of NBM data.

    Downloads just 3 forecast hours (0, 6, 12) from the most recent cycle.
    """
    print("=" * 60)
    print("NBM Data Download Test")
    print("=" * 60)

    # Setup
    template_config = NbmConusTemplateConfig()
    output_dir = Path("./test_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "test_nbm.zarr"

    # Get a recent init time (2 hours ago to account for data latency)
    init_time = pd.Timestamp.now(tz="UTC").floor("H") - timedelta(hours=2)

    print(f"\nTesting with init_time: {init_time}")
    print(f"Output path: {output_path}")

    # Create a test job with limited forecast hours
    processing_region = ProcessingRegion(
        init_time_start=init_time,
        init_time_end=init_time,
    )

    job = NbmConusForecastRegionJob(
        template_config=template_config,
        processing_region=processing_region,
        data_vars=template_config.data_vars[:3],  # Only first 3 variables
        output_path=output_path,
    )

    # Modify to only download a few forecast hours for testing
    print("\nGenerating source file coordinates (limited to 3 forecast hours)...")
    all_coords = job.generate_source_file_coords()
    test_coords = [c for c in all_coords if c.forecast_hour in [0, 6, 12]]

    print(f"Testing with {len(test_coords)} files instead of {len(all_coords)}")

    # Test downloading one file
    print("\n" + "=" * 60)
    print("Testing download of first file...")
    print("=" * 60)

    try:
        test_coord = test_coords[0]
        print(f"\nDownload URL: {test_coord.download_url()}")
        file_path = job.download_file(test_coord)
        print(f"✅ Successfully downloaded to: {file_path}")
        print(f"   File size: {file_path.stat().st_size / 1024 / 1024:.2f} MB")

        # Test reading the file
        print("\nTesting GRIB2 file reading...")
        data_dict = job.read_data(file_path, test_coord)
        print(f"✅ Successfully read {len(data_dict)} variables")

        for var_name, data in data_dict.items():
            print(f"   {var_name}: shape={data.shape}, dtype={data.dtype}")

    except Exception as e:
        print(f"❌ Error during download/read test: {e}")
        import traceback

        traceback.print_exc()
        return False

    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_minimal_download()
    sys.exit(0 if success else 1)
