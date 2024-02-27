import apache_beam as beam
import pandas as pd
import zarr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

from pangeo_forge_cordex import logon, recipe_inputs_from_iids
from pangeo_forge_cordex.catalog import catalog_entry, path
from pangeo_forge_cordex.parsing import project_from_iid
from pangeo_forge_recipes.patterns import pattern_from_file_sequence

bucket = "euro-cordex"

def get_url(bucket, prefix="", fs="s3"):
    return f"{fs}://{op.join(bucket, prefix)}"


def get_zarr_url(iid, bucket, prefix="", fs="s3"):
    return f"{op.join(get_url(bucket, prefix, fs), path(iid))}"


def get_catalog_url(bucket, project, prefix="catalog"):
    if project in ["CORDEX", "CORDEX-Reklies", "CORDEX-FPSCONV"]:
        # these go all in the same catalog (they have the same facets)
        catalog = "CORDEX"
    else:
        catalog = project
    return f"{op.join(get_url(bucket, prefix), catalog)}.csv"



def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    # This fails integration test if not imported here
    # TODO: see if --setup-file option for runner fixes this
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", consolidated=True, chunks={})
    for var in ["anom", "err", "ice", "sst"]:
        assert var in ds.data_vars
    return store


iid = "cordex.output.EUR-11.GERICS.NOAA-GFDL-GFDL-ESM2G.rcp26.r1i1p1.REMO2015.v1.mon.pr.v20180710"

sslcontext = logon()

recipe_inputs = recipe_inputs_from_iids(iid, sslcontext)

urls = recipe_inputs[iid]["urls"]

print(f"urls: {urls}")


#recipe_kwargs = recipe_inputs[iid]["recipe_kwargs"]
#pattern_kwargs = recipe_inputs[iid]["pattern_kwargs"]
#    
pattern = pattern_from_file_sequence(
        urls,
        concat_dim='time'
        )
#
recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name=f"{iid}.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | beam.Map(test_ds)
)
