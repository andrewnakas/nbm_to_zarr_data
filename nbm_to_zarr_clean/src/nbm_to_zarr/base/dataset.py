"""Base dataset orchestrator class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, TypeVar

import xarray as xr

from nbm_to_zarr.base.region_job import RegionJob, SourceFileCoord
from nbm_to_zarr.base.template_config import DataVariableConfig, TemplateConfig

SourceFileCoordT = TypeVar("SourceFileCoordT", bound=SourceFileCoord)
DataVarT = TypeVar("DataVarT", bound=DataVariableConfig)


class Dataset(ABC, Generic[SourceFileCoordT, DataVarT]):
    """Base class for dataset orchestration."""

    @property
    @abstractmethod
    def template_config(self) -> TemplateConfig[DataVarT]:
        """Return the template configuration."""
        ...

    @property
    @abstractmethod
    def region_job_class(self) -> type[RegionJob[SourceFileCoordT, DataVarT]]:
        """Return the region job class."""
        ...

    @property
    def dataset_id(self) -> str:
        """Return the dataset ID."""
        return self.template_config.dataset_attributes.id

    def operational_update(self, output_dir: Path) -> None:
        """Run an operational update of the dataset."""
        output_path = output_dir / f"{self.dataset_id}.zarr"

        print(f"\n{'='*60}")
        print(f"Starting operational update for {self.dataset_id}")
        print(f"Output path: {output_path}")
        print(f"{'='*60}\n")

        # Get jobs for operational update
        try:
            jobs = self.region_job_class.operational_update_jobs(
                template_config=self.template_config,
                data_vars=self.template_config.data_vars,
                output_path=output_path,
            )
            print(f"Created {len(jobs)} job(s) to process\n")
        except Exception as e:
            print(f"ERROR: Failed to create jobs: {e}")
            import traceback
            traceback.print_exc()
            raise

        # Process each job
        for i, job in enumerate(jobs, 1):
            print(f"\n{'='*60}")
            print(f"Processing job {i}/{len(jobs)}")
            print(f"{'='*60}\n")

            try:
                ds = job.process()

                if ds is None:
                    print("ERROR: Job returned None dataset")
                    continue

                print(f"\n{'='*60}")
                print(f"Saving dataset to Zarr...")
                print(f"{'='*60}\n")

                self._save_to_zarr(ds, output_path)

                print(f"\n✅ Successfully saved to {output_path}")

            except Exception as e:
                print(f"\nERROR: Failed to process job {i}: {e}")
                import traceback
                traceback.print_exc()
                raise

    def _save_to_zarr(self, ds: xr.Dataset, output_path: Path) -> None:
        """Save dataset to Zarr format."""
        import zarr

        print(f"Dataset info:")
        print(f"  Dimensions: {dict(ds.dims)}")
        print(f"  Variables: {list(ds.data_vars.keys())}")
        print(f"  Coordinates: {list(ds.coords.keys())}")

        # Remove timezone from datetime coordinates (Zarr doesn't support timezones)
        for coord_name in ds.coords:
            dtype_str = str(ds[coord_name].dtype)

            # Check if coordinate is datetime-like with timezone
            if 'datetime64' in dtype_str:
                # Check if timezone is in the dtype string (e.g., "datetime64[ns, UTC]")
                if any(tz in dtype_str for tz in ['UTC', 'utc', '+', '-']):
                    print(f"  Removing timezone from {coord_name} (dtype: {dtype_str})")
                    # Convert values to timezone-naive
                    values = ds[coord_name].values.astype('datetime64[ns]')
                    ds = ds.assign_coords({coord_name: values})
                else:
                    # Try pandas approach for other timezone formats
                    import pandas as pd
                    dt_index = pd.DatetimeIndex(ds[coord_name].values)
                    if dt_index.tz is not None:
                        print(f"  Removing timezone from {coord_name}")
                        values = dt_index.tz_localize(None).to_numpy()
                        ds = ds.assign_coords({coord_name: values})

        # Remove problematic attributes
        for var in ds.coords:
            if "units" in ds[var].attrs and var in ["init_time", "valid_time"]:
                del ds[var].attrs["units"]

        # Build encoding with proper compressor
        from numcodecs import Zstd

        encoding = {}
        for var in list(ds.data_vars) + list(ds.coords):
            var_encoding = {
                "compressor": Zstd(level=3),
            }

            # Get chunks from the variable if available
            if hasattr(ds[var], 'chunks') and ds[var].chunks is not None:
                # Convert dask chunks to dict
                chunks_dict = dict(zip(ds[var].dims, [c[0] if isinstance(c, tuple) else c for c in ds[var].chunks]))
                var_encoding["chunks"] = tuple(chunks_dict.get(dim, ds.dims[dim]) for dim in ds[var].dims)

            encoding[var] = var_encoding

        # Determine write mode
        mode = "a" if output_path.exists() else "w"

        print(f"Write mode: {mode}")
        print(f"Output path: {output_path}")

        # If appending, filter out encoding for existing variables
        # (xarray doesn't allow encoding to be specified for existing variables)
        if mode == "a":
            import zarr
            existing_store = zarr.open_group(str(output_path), mode='r')
            existing_vars = set(existing_store.array_keys())

            # Only keep encoding for new variables
            encoding = {k: v for k, v in encoding.items() if k not in existing_vars}

            if encoding:
                print(f"Encoding for new variables: {list(encoding.keys())}")
            else:
                print("No new variables to encode (all variables already exist)")

        try:
            # Write to Zarr (force v2 format for numcodecs compatibility)
            ds.to_zarr(
                output_path,
                mode=mode,
                encoding=encoding,
                consolidated=True,
                compute=True,
                zarr_version=2,
            )
            print(f"✅ Successfully wrote {output_path}")

        except Exception as e:
            print(f"ERROR during Zarr write: {e}")
            import traceback
            traceback.print_exc()
            raise
