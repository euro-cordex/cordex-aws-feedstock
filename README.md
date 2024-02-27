# cordex-feedstock

This repository contains the pangeo-forge feedstock to extract CORDEX data from ESGF and upload it to the EURO-CORDEX S3 bucket on AWS. The workflow utilizes the [pangeo-forge-cordex](https://github.com/euro-cordex/pangeo-forge-cordex) package for accessing the [ESGF API](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html) and prepares the input for extracting and converting datasets using [pangeo-forge-recipes](https://github.com/pangeo-forge/pangeo-forge-recipes).

## deploy

Run locally with

```
pangeo-forge-runner bake --repo=. -f=config/local.json --Bake.recipe_id=euro-cordex --Bake.job_name=test --prune
```

To open a locally create dataset

```python
import xarray as xr
import fsspec

url = "target/cordex.output.EUR-11.GERICS.NOAA-GFDL-GFDL-ESM2G.rcp26.r1i1p1.REMO2015.v1.mon.pr.v20180710.zarr"

ds = xr.open_zarr(
    store=fsspec.get_mapper(url), consolidated=True
)
```
