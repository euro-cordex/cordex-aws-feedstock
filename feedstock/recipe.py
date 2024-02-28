import apache_beam as beam
import pandas as pd
import zarr
import xarray as xr
import sys

from dataclasses import dataclass
from typing import List, Dict

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    Indexed,
    T,
)

from pangeo_forge_cordex import recipe_inputs_from_iids
from pangeo_forge_cordex.parsing import project_from_iid
from pangeo_forge_recipes.patterns import pattern_from_file_sequence


@dataclass
class Preprocessor(beam.PTransform):
    """
    Preprocessor for xarray datasets.
    Set all data_variables except for `variable_id` attrs to coord
    Add additional information

    """

    #    @staticmethod
    #    def _keep_only_variable_id(item: Indexed[T]) -> Indexed[T]:
    #        """
    #        Many netcdfs contain variables other than the one specified in the `variable_id` facet.
    #        Set them all to coords
    #        """
    #        index, ds = item
    #        print(f"Preprocessing before {ds =}")
    #        new_coords_vars = [var for var in ds.data_vars if var != ds.attrs['variable']]
    #        ds = ds.set_coords(new_coords_vars)
    #        print(f"Preprocessing after {ds =}")
    #        return index, ds

    @staticmethod
    def _sanitize_attrs(item: Indexed[T]) -> Indexed[T]:
        """Removes non-ascii characters from attributes see https://github.com/pangeo-forge/pangeo-forge-recipes/issues/586"""
        index, ds = item
        for att, att_value in ds.attrs.items():
            if isinstance(att_value, str):
                new_value = att_value.encode("utf-8", "ignore").decode()
                if new_value != att_value:
                    print(
                        f"Sanitized datasets attributes field {att}: \n {att_value} \n ----> \n {new_value}"
                    )
                    ds.attrs[att] = new_value
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (
            pcoll
            #   | "Fix coordinates" >> beam.Map(self._keep_only_variable_id)
            | "Sanitize Attrs" >> beam.Map(self._sanitize_attrs)
        )


def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    # This fails integration test if not imported here
    # TODO: see if --setup-file option for runner fixes this
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", consolidated=True, chunks={})
    for var in ["pr"]:
        assert var in ds.data_vars
    return store


## Dynamic Chunking Wrapper
def dynamic_chunking_func(ds: xr.Dataset) -> Dict[str, int]:
    import warnings

    # trying to import inside the function
    from dynamic_chunks.algorithms import (
        even_divisor_algo,
        iterative_ratio_increase_algo,
        NoMatchingChunks,
    )

    target_chunk_size = "150MB"
    target_chunks_aspect_ratio = {
        "time": 10,
        "x": 1,
        "i": 1,
        "ni": 1,
        "xh": 1,
        "nlon": 1,
        "lon": 1,
        "rlon": 1, 
        "y": 1,
        "j": 1,
        "nj": 1,
        "yh": 1,
        "nlat": 1,
        "lat": 1,
        "rlat": 1,
    }
    size_tolerance = 0.5

    try:
        target_chunks = even_divisor_algo(
            ds,
            target_chunk_size,
            target_chunks_aspect_ratio,
            size_tolerance,
            allow_extra_dims=True,
        )

    except NoMatchingChunks:
        warnings.warn(
            "Primary algorithm using even divisors along each dimension failed "
            "with. Trying secondary algorithm."
        )
        try:
            target_chunks = iterative_ratio_increase_algo(
                ds,
                target_chunk_size,
                target_chunks_aspect_ratio,
                size_tolerance,
                allow_extra_dims=True,
            )
        except NoMatchingChunks:
            raise ValueError(
                (
                    "Could not find any chunk combinations satisfying "
                    "the size constraint with either algorithm."
                )
            )
        # If something fails
        except Exception as e:
            raise e
    except Exception as e:
        raise e

    return target_chunks


iid_file = 'feedstock/iid.txt'

with open(iid_file) as f:
    iid = f.read().rstrip()

#iid = "cordex.output.EUR-11.GERICS.NOAA-GFDL-GFDL-ESM2G.rcp26.r1i1p1.REMO2015.v1.mon.pr.v20180710"

print(f"iid: {iid}")

recipe_inputs = recipe_inputs_from_iids(iid)

urls = recipe_inputs[iid]["urls"]

print(f"urls: {urls}")

pattern = pattern_from_file_sequence(urls, concat_dim="time")


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    #    | OpenWithXarray(file_type=pattern.file_type)
    | OpenWithXarray(xarray_open_kwargs={"use_cftime": True, "decode_coords": "all"})
    # set decode_coords to ensure grid_mappings are in the coordinates...
    | Preprocessor()
    | StoreToZarr(
        store_name=f"{iid}.zarr",
        combine_dims=pattern.combine_dim_keys,
        dynamic_chunking_fn=dynamic_chunking_func,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | beam.Map(test_ds)
)
