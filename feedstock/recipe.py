"""
A synthetic prototype recipe
"""

import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
)
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
    Indexed,
    T,
)
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim


# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")
"""
NCEP Global Ocean Data Assimilation System (GODAS)
"""


variables = [
    "dbss_obil",
    "dbss_obml",
    "dzdt",
    "pottmp",
    "salt",
    "sltfl",
    "sshg",
    "thflx",
    "ucur",
    "uflx",
    "vcur",
    "vflx",
]
years = range(1980, 2024)


def make_full_path(variable, time):
    return f"https://downloads.psl.noaa.gov/Datasets/godas/{variable}.{time}.nc"


variable_merge_dim = MergeDim("variable", variables)
time_concat_dim = ConcatDim("time", years)

## preprocessing transform


class Preprocess(beam.PTransform):
    """
    Set variables to be coordinates
    """

    @staticmethod
    def _set_bnds_as_coords(item: Indexed[T]) -> Indexed[T]:
        """
        The netcdf lists some of the coordinate variables as data variables.
        This is a fix which we want to apply to each dataset.
        """
        index, ds = item
        new_coords_vars = ["date", "timePlot"]
        ds = ds.set_coords(new_coords_vars)
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._set_bnds_as_coords)


pattern = FilePattern(
    make_full_path, variable_merge_dim, time_concat_dim, file_type="netcdf4"
)

GODAS = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | Preprocess()  # New preprocessor
    | StoreToZarr(
        target_chunks={"time": 120},
        store_name="GODAS.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    # | Copy(target=catalog_store_urls["small"])
)
