# cordex-aws-feedstock

## Deploy

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
