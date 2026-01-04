#!/usr/bin/env python3
"""List all unique variables in NBM GRIB2 file."""

from pathlib import Path

import rasterio


def list_variables(file_path: Path) -> None:
    """List all unique variables in GRIB2 file."""
    print(f"\n{'='*60}")
    print("NBM GRIB2 Variables Summary")
    print(f"{'='*60}\n")

    with rasterio.open(file_path) as src:
        print(f"Total bands: {src.count}\n")

        # Collect all unique elements
        elements = {}
        for i in range(1, src.count + 1):
            meta = src.tags(i)
            elem = meta.get('GRIB_ELEMENT', 'Unknown')
            short_name = meta.get('GRIB_SHORT_NAME', 'Unknown')
            comment = meta.get('GRIB_COMMENT', 'Unknown')
            unit = meta.get('GRIB_UNIT', 'Unknown')

            if elem not in elements:
                elements[elem] = {
                    'short_name': short_name,
                    'comment': comment,
                    'unit': unit,
                    'count': 0
                }
            elements[elem]['count'] += 1

        print(f"{'GRIB_ELEMENT':<15} {'Description':<40} {'Unit':<10} {'Bands'}")
        print("-" * 80)

        for elem, info in sorted(elements.items()):
            desc = info['comment'][:37] + "..." if len(info['comment']) > 40 else info['comment']
            print(f"{elem:<15} {desc:<40} {info['unit']:<10} {info['count']}")

        print(f"\nTotal unique variables: {len(elements)}")


if __name__ == "__main__":
    file_path = Path("./test_data/inspect_grib.grib2")
    if not file_path.exists():
        print(f"Error: {file_path} not found. Run inspect_grib.py first.")
    else:
        list_variables(file_path)
