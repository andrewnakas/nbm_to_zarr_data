#!/usr/bin/env python3
"""Generate intake catalog for NBM datasets."""

import json
from pathlib import Path

import xarray as xr


def generate_catalog() -> None:
    """Generate an intake catalog for all Zarr datasets."""
    catalog_dir = Path("catalog")
    catalog_dir.mkdir(parents=True, exist_ok=True)

    data_dir = Path("data")
    datasets = {}

    # Find all .zarr directories
    for zarr_path in data_dir.glob("*.zarr"):
        dataset_id = zarr_path.stem

        try:
            # Open the dataset to get metadata
            ds = xr.open_zarr(zarr_path, consolidated=True)

            datasets[dataset_id] = {
                "driver": "zarr",
                "args": {
                    "urlpath": str(zarr_path),
                    "consolidated": True,
                },
                "description": ds.attrs.get("description", ""),
                "metadata": {
                    "title": ds.attrs.get("title", ""),
                    "provider": ds.attrs.get("provider", ""),
                    "model": ds.attrs.get("model", ""),
                    "variant": ds.attrs.get("variant", ""),
                    "version": ds.attrs.get("version", ""),
                    "dimensions": {dim: int(ds.dims[dim]) for dim in ds.dims},
                    "variables": list(ds.data_vars.keys()),
                },
            }

            ds.close()

        except Exception as e:
            print(f"Warning: Could not process {zarr_path}: {e}")
            continue

    # Create intake catalog
    catalog = {
        "sources": datasets,
        "metadata": {
            "version": 1,
            "description": "NBM forecast data in Zarr format",
        },
    }

    # Write catalog
    catalog_path = catalog_dir / "catalog.json"
    with open(catalog_path, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"Catalog generated at {catalog_path}")
    print(f"Found {len(datasets)} dataset(s)")


if __name__ == "__main__":
    generate_catalog()
