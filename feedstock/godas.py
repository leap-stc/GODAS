"""
A synthetic prototype recipe
"""

import xarray as xr
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
    Copy,
)
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
)
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim


# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")
"""
NCEP Global Ocean Data Assimilation System (GODAS)
"""


# ValueError: Dimensions for level have different sizes: 0, 40 [while running '[1]: StoreToZarr/DetermineSchema/CombineGlobally(CombineXarraySchemas)/KeyWithVoid']
# follow-up, dbss_obil has no level
# pottmp has 40 levels. How do we merge.
# also, for level'less vars. What

# based on level property on noaa data access page. Should all surface be 0, and what about for unknown?


surface_level_vars = [
    "dbss_obil",  # unknown
    "dbss_obml",  # unknown
    "sltfl",  # surface
    "sshg",  # surface
    "thflx",  # surface
    "uflx",  # surface
    "vflx",  # surface
]


multi_level_vars = [
    "dzdt",  # multi
    "pottmp",  # multi
    "salt",  # multi
    "ucur",  # multi
    "vcur",  # multi
]


years = range(1980, 2024)


def make_full_path(variable, time):
    return f"https://downloads.psl.noaa.gov/Datasets/godas/{variable}.{time}.nc"


variable_merge_dim_surface = MergeDim("variable", surface_level_vars)
variable_merge_dim_multi = MergeDim("variable", multi_level_vars)

time_concat_dim = ConcatDim("time", years)

## preprocessing transform


class Preprocess(beam.PTransform):
    """Custom transform to promote vars as coords"""

    @staticmethod
    def _set_bnds_as_coords(ds: xr.Dataset) -> xr.Dataset:
        if "level" not in ds.dims:  # in ds?
            # if levels not present, assign surface level
            ds = ds.expand_dims("level").assign_coords(level=("level", [0.0]))

        new_coords_vars = ["date", "timePlot"]
        ds = ds.set_coords(new_coords_vars)
        return ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Sets bands as coords" >> beam.MapTuple(
            lambda k, v: (k, self._set_bnds_as_coords(v))
        )


pattern_surface = FilePattern(
    make_full_path, variable_merge_dim_surface, time_concat_dim, file_type="netcdf4"
)

pattern_multi_lvl = FilePattern(
    make_full_path, variable_merge_dim_multi, time_concat_dim, file_type="netcdf4"
)

GODAS_surface = (
    beam.Create(pattern_surface.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern_surface.file_type)
    | Preprocess()  # New preprocessor
    | StoreToZarr(
        target_chunks={"time": 120},
        store_name="GODAS_surface_level.zarr",
        combine_dims=pattern_surface.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["surface"])
)

GODAS_multi = (
    beam.Create(pattern_multi_lvl.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern_multi_lvl.file_type)
    | Preprocess()  # New preprocessor
    | StoreToZarr(
        target_chunks={"time": 120},
        store_name="GODAS_multi_levels.zarr",
        combine_dims=pattern_multi_lvl.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["multi"])
)
