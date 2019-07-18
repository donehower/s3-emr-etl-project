import boto3


def create_emr_cluster(new_client, S3_URI):
    """
    Starts up an EMR cluster, completes each action in Steps, then terminates.
    Change the Instances variable Keep JobFlowAliveWhenNoSteps to False to
    keep running.
    :param new_client: an EMR client
    :param S3_URI: S3 location of the script to run

    :returns: Nothing.  Prints cluster id and configurations to console.
    """
    resp = new_client.run_job_flow(
        Name="App Log ETL",
        LogUri='s3://data-lake-skd/sparkify-emr-logs/',
        ReleaseLabel='emr-5.16.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 4,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Applications=[
            {
                'Name': 'spark'
            }
        ],
        BootstrapActions=[
            {
                'Name': 'Maximize Spark Default Config',
                'ScriptBootstrapAction': {
                    'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config'
                }
            },
        ],
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'setup - copy files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/etl.py']
                }
            }
        ],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                  "spark.sql.execution.arrow.enabled": "true"
                }
            },
            {
                "Classification": "spark",
                "Properties": {
                  "maximizeResourceAllocation": "true"
                }
            }

        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    print(resp)


if __name__ == "__main__":

    # set up URI for etl script
    S3_BUCKET = 'data-lake-skd'
    S3_KEY = 'sparkify/etl.py'
    S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)

    # create client and cluster
    client = boto3.client('emr', region_name='us-west-2')
    create_emr_cluster(client, S3_URI)
