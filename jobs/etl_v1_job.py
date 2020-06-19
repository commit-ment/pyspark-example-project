"""
etl_v1_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""
import sys
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import start_spark, transform_data


def main():
    """Main ETL script definition.

    :return: None
    """

    job_name = sys.argv[1]

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name=job_name)

    # log that main ETL job is starting
    log.warn('%s is up-and-running' % job_name)

    # execute ETL pipeline
    data = extract_data(spark, config['data_source'])

    #dynamically load transformations from settings
    data = transform_data(data, config['transformations'], log)

    #data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data, config['data_output'])

    # log the success and terminate Spark application
    log.warn('%s is finished' % job_name)
    spark.stop()
    return None


def extract_data(spark, source):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    if 'csv' in source['type']:
        df = spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(source['location'])\
            .coalesce(2)
        return df
    if 'parquet' in source['type']:
        df = (
            spark.read.parquet(source['location']))
        return df
    
    return None


def load_data(df, output):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv(output['location'], mode='overwrite', header=True))
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
