"""NBM CONUS forecast dataset orchestrator."""

from __future__ import annotations

from functools import cached_property

from nbm_to_zarr.base.dataset import Dataset
from nbm_to_zarr.base.template_config import DataVariableConfig
from nbm_to_zarr.noaa.nbm_conus.forecast.region_job import (
    NbmConusForecastRegionJob,
    NbmConusSourceFileCoord,
)
from nbm_to_zarr.noaa.nbm_conus.forecast.template_config import NbmConusTemplateConfig


class NbmConusForecastDataset(Dataset[NbmConusSourceFileCoord, DataVariableConfig]):
    """Orchestrator for NBM CONUS forecast data."""

    @cached_property
    def template_config(self) -> NbmConusTemplateConfig:
        """Return the template configuration."""
        return NbmConusTemplateConfig()

    @property
    def region_job_class(self) -> type[NbmConusForecastRegionJob]:
        """Return the region job class."""
        return NbmConusForecastRegionJob
