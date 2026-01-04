"""Base classes for NBM data processing."""

from nbm_to_zarr.base.dataset import Dataset
from nbm_to_zarr.base.region_job import ProcessingRegion, RegionJob, SourceFileCoord
from nbm_to_zarr.base.template_config import (
    CoordinateConfig,
    DatasetAttributes,
    DataVariableConfig,
    TemplateConfig,
)

__all__ = [
    "CoordinateConfig",
    "DataVariableConfig",
    "DatasetAttributes",
    "Dataset",
    "ProcessingRegion",
    "RegionJob",
    "SourceFileCoord",
    "TemplateConfig",
]
