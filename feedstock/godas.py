

# pangeo-forge-runner bake --config configs/config_local_hub.py  --repo='.' --Bake.job_name=test --Bake.recipe_id 'GODAS_multi'
import xarray as xr 
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls, Copy
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
"dbss_obil", # unknown
"dbss_obml",#unknown
"sltfl",#surface
"sshg",#surface #
"thflx",#surface
"uflx",#surface
"vflx" #surface
]


multi_level_vars = [
"dzdt", #multi
"pottmp",#multi
"salt",#multi
"ucur",#multi
"vcur",#multi
]

# does full surface work?
# no, fails with: ValueError: Region (slßice(324, 340, None), slice(0, 209, None), slice(180, 360, None), slice(None, None, None)) does not align with Zarr chunks (20, 209, 180, 1). [while running 'Create|OpenURLWithFSSpec|OpenWithXarray|Preprocess|StoreToZarr|ConsolidateDimensionCoordinates|ConsolidateMetadata|Copy/StoreToZarr/StoreDatasetFragments/Map(store_dataset_fragment)']
# does full multi work?
# Data output stream buffer size 580033055 exceeds 536870912 bytes. This is likely due to a large element in a PCollection. Large elements increase pipeline RAM requirements and can cause runtime errors. Prefer multiple small elements over single large elements in PCollections. If needed, store large blobs in external storage systems, and use PCollections to pass their metadata, or use a custom coder that reduces the element's size.

# test 2 years
# test 2 vars all years

years = range(1980, 1982)


def make_full_path(variable, time):
    return f"https://downloads.psl.noaa.gov/Datasets/godas/{variable}.{time}.nc"


variable_merge_dim_surface = MergeDim("variable", surface_level_vars)
variable_merge_dim_multi = MergeDim("variable", multi_level_vars)

time_concat_dim = ConcatDim("time", years)


class Preprocess(beam.PTransform):
    """Custom transform to promote vars as coords"""

    @staticmethod
    def _set_bnds_as_coords(ds: xr.Dataset) -> xr.Dataset:
        if 'level' not in ds.dims: # in ds?
            ds = ds.transpose("time","lat","lon")
        else:
            ds = ds.transpose("time","lat","lon", "level")

            # # if levels not present, assign surface level
            # ds = ds.expand_dims("level").assign_coords(level=("level", [0.0]))

        new_coords_vars = ["date", "timePlot"]
        ds = ds.set_coords(new_coords_vars)
        # import pdb; pdb.set_trace()

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

GODASsurface = (
    beam.Create(pattern_surface.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern_surface.file_type)
    | Preprocess()  
    | StoreToZarr(
        target_chunks={'time':20, 'lat':209, 'lon':180},
        store_name="GODAS_surface_level.zarr",
        combine_dims=pattern_surface.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    # | Copy(target=catalog_store_urls["surface"])

)

GODASmulti = (
    beam.Create(pattern_multi_lvl.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern_multi_lvl.file_type)
    | Preprocess()  
    | StoreToZarr(
        # target_chunks={'time':20, 'lat':209, 'lon':180, 'level':20},
        store_name="GODAS_multiple_levels.zarr",
        combine_dims=pattern_multi_lvl.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    # | Copy(target=catalog_store_urls["multi"])

)

