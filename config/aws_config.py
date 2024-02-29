c.TargetStorage.root_path = "s3://euro-cordex/flink-testing/{job_name}"

c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"

c.InputCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
c.InputCacheStorage.fsspec_args = c.TargetStorage.fsspec_args

c.InputCacheStorage.root_path = "s3://euro-cordex/flink-testing/inputcache-data/"

c.Bake.bakery_class = "pangeo_forge_runner.bakery.flink.FlinkOperatorBakery"
