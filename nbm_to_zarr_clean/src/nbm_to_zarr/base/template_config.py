"""Base template configuration for datasets."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import Any, Generic, TypeVar

import numpy as np
import pandas as pd
import xarray as xr
from pydantic import BaseModel


class CoordinateConfig(BaseModel):
    """Configuration for a coordinate variable."""

    name: str
    dtype: str = "float64"
    chunks: dict[str, int] | None = None
    compressor: str = "zstd"
    compressor_level: int = 3
    attrs: dict[str, Any] = {}


class DataVariableConfig(BaseModel):
    """Configuration for a data variable."""

    name: str
    dtype: str = "float32"
    chunks: dict[str, int] | None = None
    compressor: str = "zstd"
    compressor_level: int = 3
    keepbits: int | None = None
    attrs: dict[str, Any] = {}


class DatasetAttributes(BaseModel):
    """Dataset-level metadata attributes."""

    id: str
    title: str
    description: str
    version: str
    provider: str
    model: str
    variant: str


DataVarT = TypeVar("DataVarT", bound=DataVariableConfig)


class TemplateConfig(ABC, BaseModel, Generic[DataVarT]):
    """Base class for dataset template configuration."""

    model_config = {"arbitrary_types_allowed": True}

    dimensions: dict[str, int]
    append_dim: str

    @property
    @abstractmethod
    def dataset_attributes(self) -> DatasetAttributes:
        """Return dataset-level attributes."""
        ...

    @abstractmethod
    def dimension_coordinates(self) -> list[CoordinateConfig]:
        """Return dimension coordinate configurations."""
        ...

    @abstractmethod
    def derive_coordinates(self, ds: xr.Dataset) -> xr.Dataset:
        """Derive additional coordinates from dimension coordinates."""
        ...

    @property
    @abstractmethod
    def coords(self) -> list[CoordinateConfig]:
        """Return all coordinate configurations (dimension + derived)."""
        ...

    @property
    @abstractmethod
    def data_vars(self) -> list[DataVarT]:
        """Return data variable configurations."""
        ...

    def append_dim_coordinates(
        self, start: pd.Timestamp, periods: int, freq: str | timedelta
    ) -> pd.DatetimeIndex:
        """Generate DatetimeIndex for the append dimension."""
        # Ensure timezone is preserved
        result = pd.date_range(start=start, periods=periods, freq=freq, tz='UTC')
        print(f"DEBUG append_dim_coordinates: start={start}, result[0]={result[0]}")
        return result

    def get_template(
        self,
        append_dim_start: pd.Timestamp,
        append_dim_periods: int,
        append_dim_freq: str | timedelta,
    ) -> xr.Dataset:
        """Create an empty xarray Dataset template with proper structure."""
        import dask.array as da

        # Create append dimension coordinates
        append_coords = self.append_dim_coordinates(
            start=append_dim_start,
            periods=append_dim_periods,
            freq=append_dim_freq,
        )

        # Create dimension coordinates
        coords_dict: dict[str, Any] = {self.append_dim: append_coords}
        for coord_config in self.dimension_coordinates():
            if coord_config.name == self.append_dim:
                continue
            coords_dict[coord_config.name] = (
                coord_config.name,
                np.arange(self.dimensions[coord_config.name], dtype=coord_config.dtype),
                coord_config.attrs,
            )

        # Create dataset with dimension coordinates
        ds = xr.Dataset(coords=coords_dict)

        # Derive additional coordinates
        ds = self.derive_coordinates(ds)

        # Create data variables using dask arrays (lazy, not materialized in memory)
        for var_config in self.data_vars:
            chunks = var_config.chunks or {dim: size for dim, size in self.dimensions.items()}
            shape = tuple(self.dimensions.get(dim, len(coords_dict[dim])) for dim in chunks.keys())
            chunk_sizes = tuple(chunks.get(dim, self.dimensions.get(dim, len(coords_dict[dim]))) for dim in chunks.keys())

            # Use dask to create a lazy array filled with NaN
            dask_array = da.full(
                shape,
                np.nan,
                dtype=var_config.dtype,
                chunks=chunk_sizes,
            )

            ds[var_config.name] = xr.DataArray(
                data=dask_array,
                dims=list(chunks.keys()),
                attrs=var_config.attrs,
            )

        # Add dataset attributes
        ds.attrs.update(self.dataset_attributes.model_dump())

        return ds

    def template_path(self, output_dir: Path) -> Path:
        """Return the path where the template should be saved."""
        return output_dir / f"{self.dataset_attributes.id}_template.zarr"
