# NOAA NBM CONUS Forecast - Space-Efficient Version

This repository provides the same functionality as the original [Nbm_to_zarr](https://github.com/andrewnakas/Nbm_to_zarr) project but with a critical modification to prevent unbounded git repository growth.

## What This Does

Automatically fetches NOAA's National Blend of Models (NBM) CONUS forecast data every hour and converts it to cloud-optimized Zarr format. The system maintains a rolling dataset with only the latest forecast, providing 19 meteorological variables at 2.5 km resolution across the Continental United States.

## Key Difference from Original Repository

**The Original Problem:**
The original repository committed Zarr data files to git every hour. Even with aggressive git cleanup (`git gc`), the repository grew unbounded because git history stores every version of every file. After weeks of operation, the repo became gigabytes in size and eventually ran out of space.

**This Solution:**
- **Data files are NOT committed to git** - They exist only in the GitHub Actions runner's working directory
- **Only metadata is version controlled** - The `catalog/` and `data_summary/` directories (small text/JSON files) are committed
- **Git repository stays tiny** - Only code, configs, and metadata are tracked
- **Same functionality** - The Zarr data is still generated hourly and available during the workflow run

## How It Works

### What Gets Committed to Git:
- Python source code
- Configuration files
- GitHub Actions workflows
- Utility scripts
- Catalog metadata (JSON/YAML)
- Data summaries (Markdown)

### What Does NOT Get Committed:
- `data/` directory - Contains Zarr stores, excluded via `.gitignore`
- GRIB2 files - Temporary download files
- Any generated binary data

### Workflow Behavior:
1. GitHub Actions runs hourly at :15 past the hour
2. Downloads latest NBM forecast from NOMADS
3. Converts GRIB2 to Zarr format in `data/` directory
4. Generates catalog and summary metadata
5. **Only commits catalog and summary files** (not data)
6. Deploys catalog to GitHub Pages
7. Data exists only in runner's working directory for that run

## Directory Structure

```
nbm_to_zarr_clean/
├── .github/workflows/    # Automation workflows
├── src/nbm_to_zarr/     # Python package
├── scripts/             # Utility scripts
├── catalog/             # Metadata catalog (committed)
├── data_summary/        # Dataset documentation (committed)
├── data/                # Zarr stores (NOT committed - .gitignored)
├── .gitignore           # Excludes data files
├── pyproject.toml       # Project configuration
├── LICENSE              # MIT License
└── README.md            # This file
```

## Installation

```bash
# Clone this repository
git clone https://github.com/YOUR_USERNAME/nbm_to_zarr_clean.git
cd nbm_to_zarr_clean

# Install dependencies (requires Python 3.12+)
pip install -e .
```

## Usage

### Command Line Interface

```bash
# Display dataset information
nbm-zarr info

# List available datasets
nbm-zarr list-datasets

# Run operational update (downloads and converts latest forecast)
nbm-zarr operational-update --dataset-id noaa-nbm-conus-forecast --output-dir ./data
```

### Accessing the Data

The Zarr store will be created at `data/noaa-nbm-conus-forecast.zarr/` and can be opened with:

```python
import xarray as xr

ds = xr.open_zarr('data/noaa-nbm-conus-forecast.zarr/')
print(ds)
```

## Available Variables

The dataset includes 19 meteorological variables:

- **Temperature**: 2m temperature, max/min temperature, dewpoint
- **Wind**: 10m/80m components (u/v), wind gusts
- **Precipitation**: Total precipitation, precipitation rate, snowfall
- **Cloud/Visibility**: Cloud cover, ceiling height, visibility
- **Radiation**: Shortwave/longwave downward flux
- **Pressure**: Surface pressure

## Data Characteristics

- **Spatial Coverage**: Continental United States (CONUS)
- **Resolution**: 2.5 km (2345 × 1597 grid points)
- **Projection**: Lambert Conformal Conic
- **Temporal**: Hourly updates, forecasts 1-84 hours ahead
- **Update Schedule**: Every hour at :15 minutes past the hour
- **Forecast Cycles**: Full 84-hour forecasts at 00z, 06z, 12z, 18z; 36-hour forecasts at other hours

## Storage Efficiency Comparison

| Repository | Typical Size After 1 Month | After 1 Year |
|------------|---------------------------|--------------|
| Original (commits data) | ~50-100 GB | 500+ GB → FAILS |
| This Version (metadata only) | ~10-20 MB | ~50-100 MB |

## GitHub Actions Workflow

The workflow runs automatically and:
- Frees up disk space on the runner
- Checks out only the latest commit (shallow clone)
- Installs dependencies via `uv`
- Runs the operational update
- Verifies Zarr store creation
- Generates metadata catalog and summary
- **Commits only catalog/summary, not data**
- Deploys catalog to GitHub Pages

## Contributing

Feel free to open issues or submit pull requests to improve the code or documentation.

## License

MIT License - See LICENSE file for details

## Acknowledgments

- Based on the original [Nbm_to_zarr](https://github.com/andrewnakas/Nbm_to_zarr) project
- Data provided by NOAA's National Blend of Models
- Accessed via NOMADS (NOAA Operational Model Archive and Distribution System)
