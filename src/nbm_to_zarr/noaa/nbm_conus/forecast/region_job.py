"""Region job processor for NBM CONUS forecast data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
import requests

from nbm_to_zarr.base.region_job import ProcessingRegion, RegionJob, SourceFileCoord
from nbm_to_zarr.base.template_config import DataVariableConfig, TemplateConfig


@dataclass
class NbmConusSourceFileCoord(SourceFileCoord):
    """Coordinate representing a single NBM CONUS forecast file."""

    init_time: pd.Timestamp
    forecast_hour: int
    region: str = "co"  # CONUS region code

    def download_url(self) -> str:
        """Return the NOMADS download URL for this file.

        Format: https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/
                blend.YYYYMMDD/HH/core/blend.tHHz.core.fXXX.co.grib2
        """
        date_str = self.init_time.strftime("%Y%m%d")
        cycle_str = self.init_time.strftime("%H")
        forecast_str = f"{self.forecast_hour:03d}"

        return (
            f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/"
            f"blend.{date_str}/{cycle_str}/core/"
            f"blend.t{cycle_str}z.core.f{forecast_str}.{self.region}.grib2"
        )

    def index_url(self) -> str:
        """Return the index file URL."""
        return f"{self.download_url()}.idx"


class NbmConusForecastRegionJob(RegionJob[NbmConusSourceFileCoord, DataVariableConfig]):
    """Process NBM CONUS forecast data for a temporal region.

    NBM forecast hour structure:
    - Hours 1-36: Hourly resolution (36 hours)
    - Hours 39-84: 3-hourly resolution (16 hours: 39, 42, 45, ..., 84)
    - Total: 52 forecast hours
    - Note: Hour 0 (analysis) is NOT available in NBM CONUS
    """

    @staticmethod
    def get_forecast_hours() -> list[int]:
        """Return list of available forecast hours.

        NBM provides:
        - Hours 1-36: Hourly
        - Hours 39-84: Every 3 hours (39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84)

        Note: Hour 0 (analysis) is NOT available in NBM CONUS.
        """
        # Hourly from 1-36
        hourly = list(range(1, 37))
        # Every 3 hours from 39-84 (not 38!)
        three_hourly = list(range(39, 85, 3))
        return hourly + three_hourly

    @staticmethod
    def get_lead_time_hours() -> list[int]:
        """Return list of all lead time hours.

        Returns:
            [1, 2, ..., 36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84]

        Note: NBM does NOT provide hour 0 (analysis).
        """
        return NbmConusForecastRegionJob.get_forecast_hours()

    @staticmethod
    def forecast_hour_to_lead_time_index(forecast_hour: int) -> int:
        """Map forecast hour to lead_time dimension index.

        Args:
            forecast_hour: The forecast hour (1-36 hourly, 39-84 every 3h)

        Returns:
            Index in the lead_time dimension (0-based)

        Examples:
            - Hour 1 -> Index 0
            - Hour 36 -> Index 35
            - Hour 39 -> Index 36
            - Hour 84 -> Index 51
        """
        if forecast_hour <= 36:
            # Hourly: hours 1-36 map to indices 0-35
            return forecast_hour - 1
        else:
            # 3-hourly: hours 39, 42, 45... map to indices 36, 37, 38...
            # Hour 39 -> index 36, Hour 42 -> index 37, etc.
            return 36 + (forecast_hour - 39) // 3

    # Variable mapping from standard names to actual NBM GRIB2 element names
    # Based on inspection of NBM GRIB2 files
    VARIABLE_MAPPING = {
        "t2m": {"grib_element": "T", "short_name": "2-HTGL"},
        "dpt2m": {"grib_element": "Td", "short_name": "2-HTGL"},
        "tmax": {"grib_element": "T", "short_name": "2-HTGL"},  # May need different logic
        "tmin": {"grib_element": "T", "short_name": "2-HTGL"},  # May need different logic
        "u10m": {"grib_element": "WindSpd", "short_name": "10-HTGL", "wind_component": "u"},
        "v10m": {"grib_element": "WindSpd", "short_name": "10-HTGL", "wind_component": "v"},
        "u80m": {"grib_element": "WindSpd", "short_name": "80-HTGL", "wind_component": "u"},
        "v80m": {"grib_element": "WindSpd", "short_name": "80-HTGL", "wind_component": "v"},
        "gust": {"grib_element": "WindGust", "short_name": "10-HTGL"},
        "tp": {"grib_element": "QPF01", "short_name": "0-SFC"},
        "prate": {"grib_element": "QPF01", "short_name": "0-SFC"},  # Same as tp, just different name
        "snow": {"grib_element": "SnowAmt01", "short_name": "0-SFC"},
        "tcc": {"grib_element": "TCDC", "short_name": "0-RESERVED"},
        "ceil": {"grib_element": "CEIL", "short_name": "0-RESERVED"},
        "vis": {"grib_element": "VIS", "short_name": "0-SFC"},
        "dswrf": {"grib_element": "DSWRF", "short_name": "0-SFC"},
        "dlwrf": {"grib_element": "DSWRF", "short_name": "0-SFC"},  # NBM may not have DLWRF
        "sp": {"grib_element": "PRES", "short_name": "0-SFC"},  # May not exist
        "rh2m": {"grib_element": "RH", "short_name": "2-HTGL"},
    }

    def generate_source_file_coords(self) -> list[NbmConusSourceFileCoord]:
        """Generate source file coordinates for the processing region.

        NBM has hourly forecasts from 1-36h, then 3-hourly from 38-84h.
        Note: f000 (analysis) files often don't exist, so we start from f001.
        """
        import os

        coords = []

        # Get the list of available forecast hours
        forecast_hours = self.get_forecast_hours()

        # Allow limiting forecast hours via environment variable for testing
        max_forecast_hour = int(os.environ.get('NBM_MAX_FORECAST_HOUR', '84'))
        forecast_hours = [h for h in forecast_hours if h <= max_forecast_hour]

        print(f"Generating source coords for {len(forecast_hours)} forecast hours:")
        print(f"  Hours 1-36: hourly")
        if max_forecast_hour > 36:
            three_hourly_count = len([h for h in forecast_hours if h > 36])
            print(f"  Hours 38-{max_forecast_hour}: every 3 hours ({three_hourly_count} files)")

        # Generate init times at hourly intervals
        current_time = self.processing_region.init_time_start
        while current_time <= self.processing_region.init_time_end:
            # For each init time, generate coords for available forecast hours
            for forecast_hour in forecast_hours:
                coords.append(
                    NbmConusSourceFileCoord(
                        init_time=current_time,
                        forecast_hour=forecast_hour,
                        region="co",
                    )
                )

            current_time += timedelta(hours=1)

        print(f"Generated {len(coords)} source file coordinates")
        return coords

    def download_file(self, source_coord: NbmConusSourceFileCoord) -> Path:
        """Download a GRIB2 file from NOMADS."""
        url = source_coord.download_url()

        # Create filename from source coordinate
        date_str = source_coord.init_time.strftime("%Y%m%d")
        cycle_str = source_coord.init_time.strftime("%H")
        forecast_str = f"{source_coord.forecast_hour:03d}"
        filename = f"blend.t{cycle_str}z.core.f{forecast_str}.{source_coord.region}.grib2"

        # Create subdirectory for this date
        download_path = self.download_dir / date_str / cycle_str
        download_path.mkdir(parents=True, exist_ok=True)

        file_path = download_path / filename

        # Download if not already cached
        if not file_path.exists():
            print(f"  Downloading {filename} ({source_coord.forecast_hour}h forecast)...")

            # Retry logic for network issues
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, stream=True, timeout=60)
                    response.raise_for_status()

                    # Write to temporary file first
                    temp_path = file_path.with_suffix('.tmp')
                    with open(temp_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    # Move to final location
                    temp_path.rename(file_path)
                    break

                except (requests.RequestException, IOError) as e:
                    if attempt < max_retries - 1:
                        print(f"  Attempt {attempt + 1} failed: {e}. Retrying...")
                        continue
                    else:
                        raise
        else:
            print(f"  Using cached {filename}")

        return file_path

    def read_data(
        self, file_path: Path, source_coord: NbmConusSourceFileCoord
    ) -> dict[str, np.ndarray]:
        """Read data from GRIB2 file using rasterio.

        Returns a dictionary mapping variable names to numpy arrays.
        """
        data_dict: dict[str, np.ndarray] = {}

        # Storage for wind data (need both speed and direction for U/V calculation)
        wind_data: dict[str, tuple[np.ndarray, np.ndarray]] = {}  # level -> (speed, direction)

        try:
            with rasterio.open(file_path) as src:
                # Store spatial metadata for coordinate extraction (if not already stored)
                if not hasattr(self, '_spatial_metadata'):
                    self._spatial_metadata = {
                        'transform': src.transform,
                        'bounds': src.bounds,
                        'crs': src.crs,
                        'width': src.width,
                        'height': src.height,
                    }

                # Read metadata for all bands
                tags_list = [src.tags(i) for i in range(1, src.count + 1)]

                # First pass: collect wind speed and direction data
                for band_idx, tags in enumerate(tags_list, start=1):
                    grib_element = tags.get("GRIB_ELEMENT", "")
                    short_name = tags.get("GRIB_SHORT_NAME", "")

                    if grib_element == "WindSpd":
                        data = src.read(band_idx)
                        nodata = src.nodata
                        if nodata is not None:
                            data = np.where(data == nodata, np.nan, data)

                        if short_name not in wind_data:
                            wind_data[short_name] = [None, None]
                        wind_data[short_name][0] = data  # speed

                    elif grib_element == "WindDir":
                        data = src.read(band_idx)
                        nodata = src.nodata
                        if nodata is not None:
                            data = np.where(data == nodata, np.nan, data)

                        if short_name not in wind_data:
                            wind_data[short_name] = [None, None]
                        wind_data[short_name][1] = data  # direction

                # Process each requested variable
                for var_config in self.data_vars:
                    if var_config.name not in self.VARIABLE_MAPPING:
                        continue

                    var_info = self.VARIABLE_MAPPING[var_config.name]
                    grib_element = var_info["grib_element"]
                    short_name = var_info.get("short_name", "")

                    # Handle wind components specially
                    if "wind_component" in var_info:
                        if short_name in wind_data:
                            speed, direction = wind_data[short_name]
                            if speed is not None and direction is not None:
                                # Convert wind direction (from) to radians
                                # Direction is "from", so add 180 to get "to" direction
                                dir_rad = np.deg2rad(direction + 180)

                                if var_info["wind_component"] == "u":
                                    # U component (east-west)
                                    data_dict[var_config.name] = speed * np.sin(dir_rad)
                                else:
                                    # V component (north-south)
                                    data_dict[var_config.name] = speed * np.cos(dir_rad)
                            else:
                                print(f"  Warning: Missing wind speed or direction for {var_config.name}")
                        else:
                            print(f"  Warning: Wind data not found for level {short_name}")
                        continue

                    # For non-wind variables, find matching band
                    found = False
                    for band_idx, tags in enumerate(tags_list, start=1):
                        elem = tags.get("GRIB_ELEMENT", "")
                        sname = tags.get("GRIB_SHORT_NAME", "")

                        # Match both element and short_name if specified
                        if elem == grib_element:
                            if not short_name or short_name in sname:
                                # Read the band data
                                data = src.read(band_idx)

                                # Handle missing values
                                nodata = src.nodata
                                if nodata is not None:
                                    data = np.where(data == nodata, np.nan, data)

                                data_dict[var_config.name] = data
                                found = True
                                break

                    if not found:
                        print(f"  Warning: Variable {var_config.name} ({grib_element}) not found in GRIB file")

        except Exception as e:
            print(f"  Error reading GRIB file {file_path}: {e}")
            raise

        return data_dict

    def _get_projection_coordinates(self) -> tuple[np.ndarray, np.ndarray]:
        """Get actual x/y projection coordinates from GRIB spatial metadata.

        Returns:
            Tuple of (x_coords, y_coords) in projection meters
        """
        if not hasattr(self, '_spatial_metadata'):
            raise RuntimeError("Spatial metadata not available. Must read at least one GRIB file first.")

        transform = self._spatial_metadata['transform']
        width = self._spatial_metadata['width']
        height = self._spatial_metadata['height']

        # Generate x coordinates (columns) - center of each pixel
        x_coords = np.array([
            transform * (col + 0.5, 0.5)
            for col in range(width)
        ])[:, 0]  # Extract x values

        # Generate y coordinates (rows) - center of each pixel
        y_coords = np.array([
            transform * (0.5, row + 0.5)
            for row in range(height)
        ])[:, 1]  # Extract y values

        print(f"Generated projection coordinates:")
        print(f"  x range: {x_coords[0]:.0f} to {x_coords[-1]:.0f} meters ({len(x_coords)} points)")
        print(f"  y range: {y_coords[0]:.0f} to {y_coords[-1]:.0f} meters ({len(y_coords)} points)")

        return x_coords.astype(np.int32), y_coords.astype(np.int32)

    def process(self) -> xr.Dataset:
        """Process the region and return the populated dataset.

        Overrides base class to set up irregular lead_time coordinate and
        extract projection coordinates from GRIB files.
        """
        # Generate source file coordinates
        source_coords = self.generate_source_file_coords()

        # Download first file to extract spatial metadata
        print("Downloading first file to extract spatial coordinates...")
        first_file = self.download_file(source_coords[0])
        _ = self.read_data(first_file, source_coords[0])  # This stores spatial metadata

        # Create dimension coordinates
        init_times = pd.date_range(
            start=self.processing_region.init_time_start,
            end=self.processing_region.init_time_end,
            freq="1h",
            tz="UTC",
        )

        print(f"DEBUG process(): init_times={init_times}")
        print(f"DEBUG process(): init_times[0]={init_times[0]}, type={type(init_times[0])}")

        # Build empty dataset
        ds = self.template_config.get_template(
            append_dim_start=init_times[0],
            append_dim_periods=len(init_times),
            append_dim_freq="1h",
        )

        print(f"DEBUG process(): ds.init_time.values={ds.init_time.values}")
        print(f"DEBUG process(): ds.init_time.values[0]={ds.init_time.values[0]}, dtype={ds.init_time.values.dtype}")

        # Replace x/y coordinates with actual projection coordinates from GRIB
        x_coords, y_coords = self._get_projection_coordinates()
        ds = ds.assign_coords(x=x_coords, y=y_coords)

        # Create irregular lead_time coordinate values for NBM
        # [1, 2, ..., 36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84]
        lead_time_hours = self.get_lead_time_hours()
        lead_times = pd.to_timedelta(lead_time_hours, unit='h')
        ds = ds.assign_coords(lead_time=lead_times)

        print(f"Lead time values: {lead_time_hours}")
        print(f"Processing {len(source_coords)} source files...")
        print(f"Total data to download: ~{len(source_coords) * 150 / 1024:.1f} GB")

        # CRITICAL: Convert dask arrays to numpy arrays to enable in-place assignment
        # If we don't do this, assignments to .values won't persist because dask arrays are lazy/immutable
        print("Converting dask arrays to numpy for data population...")
        for var_name in ds.data_vars:
            if hasattr(ds[var_name].data, 'compute'):  # Check if it's a dask array
                ds[var_name].data = ds[var_name].data.compute()
        print("✅ Arrays converted to numpy")

        # Process each source file
        processed_count = 0
        for idx, source_coord in enumerate(source_coords, 1):
            try:
                # Download file
                print(f"[{idx}/{len(source_coords)}] Downloading: {source_coord.download_url()}")
                file_path = self.download_file(source_coord)

                # Read data
                result = self.read_data(file_path, source_coord)

                # Handle backward compatibility
                if isinstance(result, tuple):
                    data_dict, metadata = result
                else:
                    data_dict = result
                    metadata = {}

                # Get indices for this source coordinate
                indices = self.get_indices(source_coord)
                init_time = indices['init_time']
                forecast_hour = indices['forecast_hour']

                # Normalize init_time to timezone-naive for comparison
                # (dataset coords are timezone-naive after Zarr conversion)
                if isinstance(init_time, pd.Timestamp) and init_time.tz is not None:
                    init_time_naive = init_time.tz_localize(None).to_datetime64()
                elif hasattr(init_time, 'tz') and init_time.tz is not None:
                    init_time_naive = pd.Timestamp(init_time).tz_localize(None).to_datetime64()
                else:
                    init_time_naive = np.datetime64(init_time, 'ns')

                # Find the init_time index
                init_idx = np.where(ds.init_time.values == init_time_naive)[0]
                if len(init_idx) == 0:
                    print(f"Warning: init_time {init_time} not found in dataset")
                    print(f"  Tried to match: {init_time_naive}")
                    print(f"  Available times: {ds.init_time.values}")
                    continue
                init_idx = init_idx[0]

                # Apply transformations and populate dataset
                for var_config in self.data_vars:
                    if var_config.name in data_dict:
                        transformed_data = self.apply_transformations(
                            {var_config.name: data_dict[var_config.name]}, var_config
                        )

                        # Populate the dataset with the data at the correct indices
                        # forecast_hour is the lead_time index (already mapped by get_indices)
                        data_array = transformed_data[var_config.name]
                        ds[var_config.name].values[init_idx, forecast_hour, :, :] = data_array

                processed_count += 1
                # Report progress more frequently for long downloads
                if processed_count % 5 == 0 or processed_count == len(source_coords):
                    pct = (processed_count / len(source_coords)) * 100
                    print(f"✅ Progress: {processed_count}/{len(source_coords)} files ({pct:.1f}%)")

            except Exception as e:
                print(f"Error processing {source_coord}: {e}")
                import traceback
                traceback.print_exc()
                continue

        print(f"Successfully processed {processed_count}/{len(source_coords)} files")
        return ds

    def get_indices(self, source_coord: NbmConusSourceFileCoord) -> dict[str, int]:
        """Get dataset indices for a source coordinate.

        Maps forecast_hour to the appropriate lead_time index based on NBM's
        irregular time structure (hourly 1-36, then 3-hourly).
        """
        return {
            'init_time': source_coord.init_time,
            'forecast_hour': self.forecast_hour_to_lead_time_index(source_coord.forecast_hour),
        }

    @classmethod
    def operational_update_jobs(
        cls,
        template_config: TemplateConfig[DataVariableConfig],
        data_vars: list[DataVariableConfig],
        output_path: Path,
    ) -> list[NbmConusForecastRegionJob]:
        """Create jobs for operational updates.

        For NBM, we process the most recent available forecast cycle.

        NOTE: NBM extended forecasts (37-84 hours) are only available for
        major 6-hourly cycles (00z, 06z, 12z, 18z). Hourly cycles (01z-05z, 07z-11z,
        13z-17z, 19z-23z) only provide forecasts out to 36 hours.
        """
        # Get current time
        now = pd.Timestamp.now(tz="UTC")

        # NBM data has some latency, so look back a few hours to ensure data availability
        # Round down to the nearest hour first
        recent_time = now.floor("h") - timedelta(hours=2)

        # Round down to the nearest 6-hour cycle (00z, 06z, 12z, 18z)
        # These are the only cycles that provide extended forecasts beyond 36 hours
        hour = recent_time.hour
        major_cycle_hour = (hour // 6) * 6  # Rounds down to 0, 6, 12, or 18
        init_time = recent_time.replace(hour=major_cycle_hour, minute=0, second=0, microsecond=0)

        print(f"Using major cycle: {init_time.strftime('%Y-%m-%d %Hz')} (ensures 84-hour forecast availability)")

        # Create a single job for the most recent forecast
        processing_region = ProcessingRegion(
            init_time_start=init_time,
            init_time_end=init_time,
        )

        return [
            cls(
                template_config=template_config,
                processing_region=processing_region,
                data_vars=data_vars,
                output_path=output_path,
            )
        ]
