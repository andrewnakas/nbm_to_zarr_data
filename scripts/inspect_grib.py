#!/usr/bin/env python3
"""Inspect NBM GRIB2 file structure to identify available variables."""

import sys
from pathlib import Path

import pandas as pd
import requests
import rioxarray


def download_grib_file(url: str, output_path: Path) -> None:
    """Download a GRIB2 file."""
    print(f"Downloading: {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded to: {output_path}")
    print(f"Size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")


def inspect_grib_file(file_path: Path) -> None:
    """Inspect all bands in a GRIB2 file."""
    print(f"\n{'='*60}")
    print(f"Inspecting GRIB2 file: {file_path}")
    print(f"{'='*60}\n")

    try:
        # Open with rioxarray to see all bands
        import rasterio

        with rasterio.open(file_path) as src:
            print(f"Number of bands: {src.count}")
            print(f"Dimensions: {src.width} x {src.height}")
            print(f"CRS: {src.crs}")
            print(f"\nBand metadata:\n")

            for i in range(1, min(src.count + 1, 50)):  # First 50 bands
                band_meta = src.tags(i)
                print(f"Band {i}:")
                print(f"  GRIB_ELEMENT: {band_meta.get('GRIB_ELEMENT', 'N/A')}")
                print(f"  GRIB_SHORT_NAME: {band_meta.get('GRIB_SHORT_NAME', 'N/A')}")
                print(f"  GRIB_COMMENT: {band_meta.get('GRIB_COMMENT', 'N/A')}")
                print(f"  GRIB_UNIT: {band_meta.get('GRIB_UNIT', 'N/A')}")

                # Check for forecast time info
                if 'GRIB_FORECAST_SECONDS' in band_meta:
                    forecast_hours = int(band_meta['GRIB_FORECAST_SECONDS']) / 3600
                    print(f"  Forecast hour: {forecast_hours}")

                print()

                # Group by element for summary
                if i == min(src.count, 50):
                    print(f"\n{'='*60}")
                    print("Summary of available variables:")
                    print(f"{'='*60}\n")

                    elements = {}
                    for j in range(1, src.count + 1):
                        meta = src.tags(j)
                        elem = meta.get('GRIB_ELEMENT', 'Unknown')
                        short_name = meta.get('GRIB_SHORT_NAME', 'Unknown')
                        comment = meta.get('GRIB_COMMENT', 'Unknown')

                        if elem not in elements:
                            elements[elem] = {
                                'short_name': short_name,
                                'comment': comment,
                                'count': 0
                            }
                        elements[elem]['count'] += 1

                    for elem, info in sorted(elements.items()):
                        print(f"{elem} ({info['short_name']}): {info['count']} bands")
                        print(f"  Description: {info['comment']}")

    except Exception as e:
        print(f"Error inspecting GRIB file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    # Download a recent NBM GRIB2 file
    current_time = pd.Timestamp.now(tz="UTC")

    # Try most recent cycle (with 2-hour lookback for data latency)
    init_time = (current_time - pd.Timedelta(hours=2)).floor("h")

    # Build URL for f001 file (more likely to exist than f000)
    url = (
        f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/"
        f"blend.{init_time.strftime('%Y%m%d')}/{init_time.strftime('%H')}/core/"
        f"blend.t{init_time.strftime('%H')}z.core.f001.co.grib2"
    )

    output_path = Path("./test_data/inspect_grib.grib2")

    try:
        download_grib_file(url, output_path)
        inspect_grib_file(output_path)

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            # Try previous cycle
            init_time = init_time - pd.Timedelta(hours=1)
            url = (
                f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/"
                f"blend.{init_time.strftime('%Y%m%d')}/{init_time.strftime('%H')}/core/"
                f"blend.t{init_time.strftime('%H')}z.core.f001.co.grib2"
            )
            print(f"\nTrying previous cycle: {url}")
            download_grib_file(url, output_path)
            inspect_grib_file(output_path)
        else:
            raise


if __name__ == "__main__":
    main()
