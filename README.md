

## Using the Files
- `launch-etl-cluster.py`
    * Can be run from the command line if AWS access key and secret access key are configured locally. Alternatively, this script can be used as the handler of a lambda function.
    * Performs the following:
        1. Spins up an EMR cluster
        2. Copies the ETL script from S3 to the leader node
        3. Executes the ETL script via a `spark-submit` job
        4. Terminates the cluster upon completion of all steps. [The cluster can also be set to continue running after completion by changing the instance setting of `KeepJobFlowAliveWhenNoSteps` to `True`.]

- `etl.py`
    * Loads files from S3.
    * Transforms data into a star schema.
    * Writes each table as a parquet file back to s3.