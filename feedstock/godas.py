# pangeo-forge-runner bake --config configs/config_local_hub.py  --repo='.' --Bake.job_name=test --Bake.recipe_id 'GODAS_multi'
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


catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


surface_level_vars = [
    "dbss_obil",  # unknown
    "dbss_obml",  # unknown
    "sltfl",  # surface
    "sshg",  # surface #
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



pattern_surface = FilePattern(
    make_full_path, variable_merge_dim_surface, time_concat_dim, file_type="netcdf4"
)

pattern_multi_lvl = FilePattern(
    make_full_path, variable_merge_dim_multi, time_concat_dim, file_type="netcdf4"
)

GODASsurface = (
    beam.Create(pattern_surface.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern_surface.file_type, xarray_open_kwargs={'drop_variables':["date","timePlot"]})
    | StoreToZarr(
        store_name="GODAS_surface_level.zarr",
        target_chunks={"time": 720, "lat": 209, "lon": 180},
        combine_dims=pattern_surface.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["surface"])
)

GODASmulti = (
    beam.Create(pattern_multi_lvl.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern_multi_lvl.file_type)
    | Preprocess()
    | StoreToZarr(
        target_chunks={"time": 36, "lat": 209, "lon": 180, "level": 20},
        store_name="GODAS_multiple_levels.zarr",
        combine_dims=pattern_multi_lvl.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["multi"])
)
