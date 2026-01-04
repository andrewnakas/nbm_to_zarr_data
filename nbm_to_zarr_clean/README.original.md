# NOAA NBM CONUS Forecast

![Status](https://img.shields.io/badge/status-updating-success)

---

## Dataset Specifications

- **Spatial domain**: Continental United States (CONUS)
- **Spatial resolution**: 2.5 km (~0.0225 degrees)
- **Projection**: Lambert Conformal Conic
- **Grid dimensions**: 2345 × 1597 points
- **Time domain**: Forecasts initialized hourly from 2025-11-15 00:00:00 UTC to Present
- **Time resolution**: Forecasts initialized every hour
- **Forecast domain**: 1-84 hours (up to 3.5 days) ahead
- **Forecast resolution**: Hourly for hours 1-36, then 3-hourly (39, 42, 45, ..., 84h)

---

## About

The **National Blend of Models (NBM)** is a nationally consistent and skillful suite of calibrated forecast guidance based on a blend of both NWS and non-NWS numerical weather prediction model data and post-processed model guidance. The NBM combines forecast information from multiple numerical weather prediction systems and applies sophisticated calibration and blending techniques to create highly accurate, seamless forecasts across the United States.

This dataset provides hourly-updating NBM CONUS forecast data in cloud-optimized Zarr format, covering **19 key meteorological variables** including temperature, wind, precipitation, cloud cover, and radiation fields at high spatial resolution (2.5 km).

---

## Related Datasets

- [NOAA GFS Forecast](https://github.com/andrewnakas/ak_hrrr_to_zarr) - Global 0.25° resolution
- [NOAA HRRR Alaska](https://github.com/andrewnakas/ak_hrrr_to_zarr) - 3 km Alaska regional
- [NOAA NAM CONUS](https://dynamical.org/catalog/noaa-nam-conus-forecast/) - 12 km CONUS regional

---

## Examples

### Python (xarray)

```python
import xarray as xr

# Open the dataset
ds = xr.open_zarr(
    "https://github.com/andrewnakas/Nbm_to_zarr/raw/main/data/noaa-nbm-conus-forecast.zarr",
    consolidated=True
)

# Select latest forecast for 2-meter temperature
t2m = ds['t2m'].isel(init_time=-1)

# Get temperature 6 hours into the forecast
t2m_6h = t2m.sel(lead_time='6h', method='nearest')

print(f"Forecast shape: {t2m_6h.shape}")
print(f"Temperature range: {t2m_6h.min().values:.1f} - {t2m_6h.max().values:.1f} K")
```

---

## Dimensions

| Dimension | Type | Size | Description |
|-----------|------|------|-------------|
| `init_time` | datetime64[ns] | dynamic | Forecast initialization time (UTC) |
| `lead_time` | timedelta64[ns] | 52 | Forecast lead time (1-36h hourly, then 39-84h every 3h) |
| `y` | int32 | 1597 | North-south grid coordinate in projection meters (Lambert Conformal) |
| `x` | int32 | 2345 | East-west grid coordinate in projection meters (Lambert Conformal) |

**Notes**:
- The `lead_time` dimension has irregular spacing: `[1, 2, ..., 36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84]` hours. Hour 0 (analysis) is NOT available in NBM CONUS.
- The `x` and `y` coordinates contain actual projection coordinates in meters (not array indices), enabling direct spatial selection with `ds.sel(x=meters, y=meters, method='nearest')`.

---

## Variables

| Variable | Description | Units | Dimensions |
|----------|-------------|-------|------------|
| `t2m` | 2-meter temperature | K | (init_time, lead_time, y, x) |
| `dpt2m` | 2-meter dewpoint temperature | K | (init_time, lead_time, y, x) |
| `tmax` | Maximum temperature | K | (init_time, lead_time, y, x) |
| `tmin` | Minimum temperature | K | (init_time, lead_time, y, x) |
| `rh2m` | 2-meter relative humidity | % | (init_time, lead_time, y, x) |
| `u10m` | 10-meter u-component of wind | m s⁻¹ | (init_time, lead_time, y, x) |
| `v10m` | 10-meter v-component of wind | m s⁻¹ | (init_time, lead_time, y, x) |
| `u80m` | 80-meter u-component of wind | m s⁻¹ | (init_time, lead_time, y, x) |
| `v80m` | 80-meter v-component of wind | m s⁻¹ | (init_time, lead_time, y, x) |
| `gust` | Wind gust | m s⁻¹ | (init_time, lead_time, y, x) |
| `tp` | Total precipitation | kg m⁻² | (init_time, lead_time, y, x) |
| `prate` | Precipitation rate | kg m⁻² s⁻¹ | (init_time, lead_time, y, x) |
| `snow` | Snow accumulation | kg m⁻² | (init_time, lead_time, y, x) |
| `tcc` | Total cloud cover | % | (init_time, lead_time, y, x) |
| `ceil` | Ceiling height | m | (init_time, lead_time, y, x) |
| `vis` | Visibility | m | (init_time, lead_time, y, x) |
| `dswrf` | Downward shortwave radiation flux | W m⁻² | (init_time, lead_time, y, x) |
| `dlwrf` | Downward longwave radiation flux | W m⁻² | (init_time, lead_time, y, x) |
| `sp` | Surface pressure | Pa | (init_time, lead_time, y, x) |

---

## Data Access

### Direct Download

The latest forecast data is available in the `data/` directory:
- **Zarr Format**: `data/noaa-nbm-conus-forecast.zarr`
- **Catalog**: `catalog/catalog.json`
- **Summary**: `data_summary/README.md`

### Automated Updates

This repository automatically updates every hour at :15 minutes past the hour via GitHub Actions. The workflow:
1. Downloads the latest NBM forecast from NOMADS (uses major 6-hourly cycles: 00z, 06z, 12z, 18z)
2. Converts GRIB2 data to cloud-optimized Zarr format
3. Maintains a rolling dataset (keeps only the latest forecast)
4. Generates catalog metadata and documentation

**Note**: The full 84-hour forecast is only available from major 6-hourly cycles (00z, 06z, 12z, 18z). Hourly cycles (01z-05z, 07z-11z, 13z-17z, 19z-23z) only provide forecasts out to 36 hours.

---

## Installation

```bash
# Clone the repository
git clone https://github.com/andrewnakas/Nbm_to_zarr.git
cd Nbm_to_zarr

# Install with pip
pip install -e .
```

### Command Line Interface

```bash
# Show dataset information
nbm-zarr info noaa-nbm-conus-forecast

# List available datasets
nbm-zarr list-datasets

# Run operational update (downloads latest forecast)
nbm-zarr operational-update --dataset-id noaa-nbm-conus-forecast --output-dir ./data
```

---

## Technical Details

### Compression

Data is stored using Zarr v2 format with Zstd compression (level 3) and bit-rounding for optimal compression ratios while maintaining numerical accuracy:
- Temperature variables: 12 bits
- Wind variables: 10 bits
- Precipitation variables: 14 bits
- Cloud/visibility: 8-10 bits

### Chunking Strategy

Optimized for time-series access and spatial subsetting:
- `init_time`: 1
- `lead_time`: 53 (all lead times in one chunk)
- `y`: 266 (≈1597/6)
- `x`: 391 (≈2345/6)

Target chunk size: ~3-5 MB compressed

### Projection Information

The data uses Lambert Conformal Conic projection with the following parameters:
- Standard parallels: 25°N
- Central meridian: -95°W
- Latitude of origin: 25°N
- False easting/northing: 0 m
- Earth radius: 6,371,200 m

PROJ.4 string:
```
+proj=lcc +lat_1=25 +lat_2=25 +lat_0=25 +lon_0=-95 +x_0=0 +y_0=0 +R=6371200 +units=m +no_defs
```

---

## Acknowledgments

- **Data Provider**: [NOAA National Centers for Environmental Prediction](https://www.ncep.noaa.gov/)
- **Data Source**: [NOMADS - NBM CONUS](https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/)
- **Inspiration**: Architecture patterns from [dynamical.org](https://dynamical.org/)

### Citation

If you use this dataset in your research, please cite:

```
NOAA National Blend of Models (NBM) CONUS Forecast
National Centers for Environmental Prediction (NCEP)
Accessed via: https://github.com/andrewnakas/Nbm_to_zarr
```

---

## Contact

For questions or issues regarding this dataset, please [open an issue](https://github.com/andrewnakas/Nbm_to_zarr/issues) on GitHub.

---

## License

This repository is licensed under the MIT License. The NBM data is provided by NOAA and is in the public domain.

**Last Updated**: 2025-11-15 (automatically updated hourly)
