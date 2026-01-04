# Quick Setup Guide

## Prerequisites

- Python 3.12 or higher
- Git
- GitHub account (for automated updates)

## Local Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/nbm_to_zarr_clean.git
   cd nbm_to_zarr_clean
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install the package:**
   ```bash
   pip install -e .
   ```

4. **Test the installation:**
   ```bash
   nbm-zarr --help
   nbm-zarr info
   ```

## Running Your First Update

1. **Create data directory:**
   ```bash
   mkdir -p data
   ```

2. **Run operational update:**
   ```bash
   nbm-zarr operational-update --dataset-id noaa-nbm-conus-forecast --output-dir ./data
   ```

   This will:
   - Download the latest NBM forecast from NOMADS
   - Convert GRIB2 files to Zarr format
   - Save to `data/noaa-nbm-conus-forecast.zarr/`

3. **Verify the data:**
   ```python
   import xarray as xr
   ds = xr.open_zarr('data/noaa-nbm-conus-forecast.zarr/')
   print(ds)
   ```

## GitHub Actions Setup

1. **Push to GitHub:**
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/nbm_to_zarr_clean.git
   git push -u origin main
   ```

2. **Enable GitHub Actions:**
   - Go to your repository settings
   - Navigate to Actions → General
   - Enable "Read and write permissions" for workflows

3. **Enable GitHub Pages (for catalog):**
   - Go to Settings → Pages
   - Source: GitHub Actions
   - The workflow will automatically deploy the catalog

4. **Workflow will run automatically:**
   - Every hour at :15 past the hour
   - Can also trigger manually from Actions tab

## Verifying Space Efficiency

After the workflow runs a few times, check your repo size:

```bash
# In your local clone
git pull
du -sh .git

# You should see something like:
# 300K  .git
```

Compare this to the original repo which would be growing by GBs per day!

## Troubleshooting

### "No module named 'nbm_to_zarr'"

Make sure you installed the package:
```bash
pip install -e .
```

### "Permission denied" on GitHub Actions

Ensure workflow has write permissions:
- Settings → Actions → General → Workflow permissions
- Select "Read and write permissions"

### Data directory not created

The workflow creates `data/` automatically. Locally, create it manually:
```bash
mkdir -p data catalog data_summary
```

### Workflow fails with "disk space" error

This shouldn't happen with the clean version, but if it does:
- Check that `data/` is in `.gitignore`
- Verify the workflow isn't committing data (check git log)
- Ensure cleanup scripts are running

## Next Steps

- **Customize variables**: Edit `src/nbm_to_zarr/noaa/nbm_conus/forecast/template_config.py`
- **Adjust schedule**: Modify `.github/workflows/update-data.yml` cron schedule
- **Add cloud storage**: Integrate S3/GCS upload in the workflow
- **Set up monitoring**: Add notifications for workflow failures

## Support

- Check `README.md` for detailed information
- Review `CHANGES.md` to understand modifications from original
- Open an issue on GitHub for problems

## Development

### Running tests:
```bash
python scripts/test_basic.py
python scripts/test_download.py
```

### Inspecting GRIB files:
```bash
python scripts/inspect_grib.py <path-to-grib-file>
```

### Listing available variables:
```bash
python scripts/list_grib_variables.py
```

### Manual catalog generation:
```bash
python scripts/generate_catalog.py
python scripts/create_summary.py
```

## License

MIT License - see LICENSE file
