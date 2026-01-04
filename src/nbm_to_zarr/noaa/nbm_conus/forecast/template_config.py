"""Template configuration for NBM CONUS forecast data."""

from __future__ import annotations

from functools import cached_property

import numpy as np
import pandas as pd
import xarray as xr

from nbm_to_zarr.base.template_config import (
    CoordinateConfig,
    DatasetAttributes,
    DataVariableConfig,
    TemplateConfig,
)


class NbmConusTemplateConfig(TemplateConfig[DataVariableConfig]):
    """Template configuration for NBM CONUS forecast dataset.

    Grid specifications:
    - Dimensions: 2345 x 1597 points
    - Resolution: 2.5 kilometers
    - Projection: Lambert Conformal
    - Domain: CONUS (Continental United States)

    Temporal configuration:
    - Update frequency: Hourly
    - Forecast hours: 1-36 hourly, then 39-84 every 3 hours
    - Total: 52 lead times (36 hourly + 16 three-hourly)
    - Note: Hour 0 (analysis) is NOT available in NBM
    """

    dimensions: dict[str, int] = {
        "init_time": 1,  # Will be extended dynamically
        "lead_time": 52,  # 1-36h hourly + 39-84h every 3h (NO hour 0)
        "y": 1597,
        "x": 2345,
    }
    append_dim: str = "init_time"

    @cached_property
    def dataset_attributes(self) -> DatasetAttributes:
        """Return dataset-level metadata."""
        return DatasetAttributes(
            id="noaa-nbm-conus-forecast",
            title="NOAA National Blend of Models (NBM) CONUS Forecast",
            description=(
                "Hourly forecast data from the National Blend of Models (NBM) for "
                "the Continental United States (CONUS) on a 2.5 km Lambert Conformal grid"
            ),
            version="4.3",
            provider="NOAA/NWS/NCEP",
            model="NBM",
            variant="CONUS",
        )

    def dimension_coordinates(self) -> list[CoordinateConfig]:
        """Return dimension coordinate configurations."""
        return [
            CoordinateConfig(
                name="init_time",
                dtype="datetime64[ns]",
                attrs={
                    "long_name": "Forecast initialization time",
                    "standard_name": "forecast_reference_time",
                },
            ),
            CoordinateConfig(
                name="lead_time",
                dtype="timedelta64[ns]",
                attrs={
                    "long_name": "Forecast lead time",
                    "standard_name": "forecast_period",
                },
            ),
            CoordinateConfig(
                name="y",
                dtype="int32",
                attrs={
                    "long_name": "y-coordinate in projection",
                    "units": "meters",
                },
            ),
            CoordinateConfig(
                name="x",
                dtype="int32",
                attrs={
                    "long_name": "x-coordinate in projection",
                    "units": "meters",
                },
            ),
        ]

    def derive_coordinates(self, ds: xr.Dataset) -> xr.Dataset:
        """Derive additional coordinates from dimension coordinates."""
        # Add valid_time coordinate
        if "init_time" in ds.coords and "lead_time" in ds.coords:
            # Convert init_time to datetime64[ns] without timezone for numpy operations
            # Use pandas to handle timezone-aware datetimes properly
            init_time_values = pd.DatetimeIndex(ds.coords["init_time"].values)
            print(f"DEBUG derive_coordinates: original init_time_values={init_time_values}, tz={init_time_values.tz}")

            # Remove timezone if present
            if init_time_values.tz is not None:
                init_time_values = init_time_values.tz_localize(None)
                print(f"DEBUG derive_coordinates: after tz_localize(None)={init_time_values}")

            # Convert to numpy array
            init_time_values = init_time_values.to_numpy(dtype='datetime64[ns]')
            print(f"DEBUG derive_coordinates: final init_time_values={init_time_values}")

            # CRITICAL: Update the init_time coordinate to be timezone-naive datetime64[ns]
            # This ensures it can be properly compared later
            ds = ds.assign_coords(init_time=init_time_values)
            print(f"DEBUG derive_coordinates: ds.init_time after assign={ds.init_time.values}, dtype={ds.init_time.dtype}")

            # Now we can safely add datetime64 + timedelta64
            valid_time_values = (
                init_time_values[:, np.newaxis]
                + ds.coords["lead_time"].values[np.newaxis, :]
            )

            ds = ds.assign_coords(
                valid_time=(
                    ["init_time", "lead_time"],
                    valid_time_values,
                    {
                        "long_name": "Forecast valid time",
                        "standard_name": "time",
                    },
                )
            )

        # Add spatial_ref coordinate for projection information
        ds = ds.assign_coords(
            spatial_ref=(
                [],
                0,
                {
                    "grid_mapping_name": "lambert_conformal_conic",
                    "standard_parallel": [25.0, 25.0],
                    "longitude_of_central_meridian": -95.0,
                    "latitude_of_projection_origin": 25.0,
                    "false_easting": 0.0,
                    "false_northing": 0.0,
                    "earth_radius": 6371200.0,
                    "proj4": (
                        "+proj=lcc +lat_1=25 +lat_2=25 +lat_0=25 +lon_0=-95 "
                        "+x_0=0 +y_0=0 +R=6371200 +units=m +no_defs"
                    ),
                },
            )
        )

        return ds

    @cached_property
    def coords(self) -> list[CoordinateConfig]:
        """Return all coordinate configurations."""
        return self.dimension_coordinates()

    @cached_property
    def data_vars(self) -> list[DataVariableConfig]:
        """Return data variable configurations.

        Variables are selected based on common use cases and data availability.
        Each variable includes chunking optimized for ~3-5MB compressed chunks.
        """
        # Common chunking strategy
        chunks = {
            "init_time": 1,
            "lead_time": 53,  # All lead times in one chunk
            "y": 266,  # ~1597/6
            "x": 391,  # ~2345/6
        }

        return [
            # Temperature variables
            DataVariableConfig(
                name="t2m",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "2-meter temperature",
                    "units": "K",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="dpt2m",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "2-meter dewpoint temperature",
                    "units": "K",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="tmax",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "Maximum temperature",
                    "units": "K",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="tmin",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "Minimum temperature",
                    "units": "K",
                    "grid_mapping": "spatial_ref",
                },
            ),
            # Wind variables
            DataVariableConfig(
                name="u10m",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "10-meter u-component of wind",
                    "units": "m s-1",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="v10m",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "10-meter v-component of wind",
                    "units": "m s-1",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="u80m",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "80-meter u-component of wind",
                    "units": "m s-1",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="v80m",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "80-meter v-component of wind",
                    "units": "m s-1",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="gust",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "Wind gust",
                    "units": "m s-1",
                    "grid_mapping": "spatial_ref",
                },
            ),
            # Precipitation variables
            DataVariableConfig(
                name="tp",
                dtype="float32",
                chunks=chunks,
                keepbits=14,
                attrs={
                    "long_name": "Total precipitation",
                    "units": "kg m-2",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="prate",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "Precipitation rate",
                    "units": "kg m-2 s-1",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="snow",
                dtype="float32",
                chunks=chunks,
                keepbits=14,
                attrs={
                    "long_name": "Snow accumulation",
                    "units": "kg m-2",
                    "grid_mapping": "spatial_ref",
                },
            ),
            # Cloud and visibility
            DataVariableConfig(
                name="tcc",
                dtype="float32",
                chunks=chunks,
                keepbits=8,
                attrs={
                    "long_name": "Total cloud cover",
                    "units": "%",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="ceil",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "Ceiling height",
                    "units": "m",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="vis",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "Visibility",
                    "units": "m",
                    "grid_mapping": "spatial_ref",
                },
            ),
            # Radiation
            DataVariableConfig(
                name="dswrf",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "Downward shortwave radiation flux",
                    "units": "W m-2",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="dlwrf",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "Downward longwave radiation flux",
                    "units": "W m-2",
                    "grid_mapping": "spatial_ref",
                },
            ),
            # Pressure and humidity
            DataVariableConfig(
                name="sp",
                dtype="float32",
                chunks=chunks,
                keepbits=12,
                attrs={
                    "long_name": "Surface pressure",
                    "units": "Pa",
                    "grid_mapping": "spatial_ref",
                },
            ),
            DataVariableConfig(
                name="rh2m",
                dtype="float32",
                chunks=chunks,
                keepbits=10,
                attrs={
                    "long_name": "2-meter relative humidity",
                    "units": "%",
                    "grid_mapping": "spatial_ref",
                },
            ),
        ]
