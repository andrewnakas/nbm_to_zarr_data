#!/usr/bin/env python3
"""Create a summary document of available datasets."""

from datetime import datetime
from pathlib import Path

import xarray as xr


def format_size(size_bytes: int) -> str:
    """Format size in bytes to human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def get_directory_size(path: Path) -> int:
    """Calculate total size of directory."""
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def create_summary() -> None:
    """Create a summary document of available datasets."""
    data_dir = Path("data")
    summary_dir = Path("data_summary")
    summary_dir.mkdir(parents=True, exist_ok=True)

    summary_lines = [
        "# NBM Data Summary\n",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n",
        "\n## Available Datasets\n",
    ]

    # Process each Zarr dataset
    for zarr_path in sorted(data_dir.glob("*.zarr")):
        try:
            ds = xr.open_zarr(zarr_path, consolidated=True)

            # Get basic info
            dataset_id = zarr_path.stem
            size = get_directory_size(zarr_path)

            summary_lines.append(f"\n### {dataset_id}\n")
            summary_lines.append(f"\n**Storage Size:** {format_size(size)}\n")

            # Add metadata
            if "title" in ds.attrs:
                summary_lines.append(f"\n**Title:** {ds.attrs['title']}\n")
            if "description" in ds.attrs:
                summary_lines.append(f"\n**Description:** {ds.attrs['description']}\n")

            # Add dimension info
            summary_lines.append("\n**Dimensions:**\n")
            for dim, size in ds.dims.items():
                summary_lines.append(f"- {dim}: {size}\n")

            # Add time range if available
            if "init_time" in ds.dims:
                init_times = ds["init_time"].values
                summary_lines.append(
                    f"\n**Forecast Initialization Times:** "
                    f"{len(init_times)} cycles\n"
                )
                summary_lines.append(
                    f"- First: {str(init_times[0])}\n"
                )
                summary_lines.append(
                    f"- Last: {str(init_times[-1])}\n"
                )

            # Add variable list
            summary_lines.append(f"\n**Variables ({len(ds.data_vars)}):**\n")
            for var in sorted(ds.data_vars.keys()):
                long_name = ds[var].attrs.get("long_name", var)
                summary_lines.append(f"- `{var}`: {long_name}\n")

            ds.close()

        except Exception as e:
            summary_lines.append(f"\n### {zarr_path.name}\n")
            summary_lines.append(f"\n**Error:** Could not read dataset: {e}\n")

    # Write summary
    summary_path = summary_dir / "README.md"
    with open(summary_path, "w") as f:
        f.writelines(summary_lines)

    print(f"Summary created at {summary_path}")


if __name__ == "__main__":
    create_summary()
