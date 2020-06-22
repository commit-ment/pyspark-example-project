# PySpark Example Project - Forked

This project is based on a [repo](https://github.com/AlexIoannides/pyspark-example-project). It is recommended to first go through the original description in order to get the full grasp of the idea and in order to proceed with the design and architecture changes applied here.

Basic idea from the original repository is to address the following topics:

- how to structure ETL code in such a way that it can be easily tested and debugged;
- how to pass configuration parameters to a PySaprk job;
- how to handle dependencies on another modules and packages;
- what constitutes a ‘meaningful’ test for an ETL job;

Changes applied in this version of the original repository consist of decoupling transformations from the initial set up in order to have an independent transformation logic, which is defined earlier and invoked from the .config file. A series of transformations are then dynamically loaded and executed in a serial manner, where a dataframe outputted by a transformation is the one received by the next transformation defined in the config file. Configuration for the transformation allows the possibility to have custom named parameters passed to the transformation implementation.


## ETL Project Structure

The structure differs from the original in the manner that we have a separate `transformations` folder which should contain a script per transformation. Such a file contains an atomic implementation of the transformation within one `process` method receiving a dataframe as the first parameter and following `parameters` (which can be specified within the configuration), and optionally a logger. This method returns the dataframe to which the required transformation is applied and which is used as input in the next transformation step defined in the configuration.

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |   |-- etl_v1_employees_data_config.json
 |   |-- etl_v1_retail_data_config.json
 |   |-- etl_v1_retail_data_test_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- jobs/
 |   |-- etl_job.py
 |   |-- etl_v1_job.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- employees/
 |   |-- | -- employees_report/
 |   |-- | -- retail-data/
 |   |-- | -- retail-data_report/
 |   |-- test_etl_job.py
 |   |-- test_etl_v1_job.py
 |-- transformations/
 |   |-- capitalise.py
 |   |-- items_sold_filter_country.py
 |   |-- items_sold_per_country.py
 |   |-- steps.py 
 |   build_dependencies.sh
 |   packages.zip
 |   transformations.zip
 |   Pipfile
 |   Pipfile.lock
```

It’s important to note that dynamic invoking of transformations is defined within the `dependencies/spark.py` `transform_data` method. Having that in mind, it’s enough to call this method upon defining the data loading part in the custom job script passing the data frame and configuration transformations json section.

## Structure of an ETL Job

All description from the original repository applies here, with one important update, which concerns the transformation dynamic loading and execution process. Instead of defying the transformation method within the job script, it’s required to import the `transform_data` method from `dependencies.spark` and to invoke it with a loaded data frame, transformation section of the configuration and logger optionally.

Also, another change is that the job name is to be specified from the command line.


## Configuration Parameters

Configuration section is the one with added complexity in order to facilitate the dynamic invocation of transformations. All definitions in this section defined in the original repository apply with the addition of three configuration keys.

### Data source location

```json

“data_source”:
{
	"location": "-location of the data",
	"type": "parquet | csv | etc."
}

```
Data type loading implementation logic should be implemented in the custom job script. Current template `etl_v1_job.py` contains a low level example given that the focus was mostly on the transformation decoupling.

### Transformations

```json
“transformations”:
{
    "transformation_name (script name)":
    {
         "-transformation related parameters in json format"
    },
	"transformation_name (script name)":
    {
         "-transformation related parameters in json format"
    }
}
```

### Data output

```json
"data_output":
{
	"location": "-location of the output"
}

```

Optionally if you wish to save the transformed data somewhere, you can specify the location in the configuration file.


## Packaging ETL Job Dependencies

All packaging logic applies with the addition of another file, `transformations.zip`. This folder contains all the transformation scripts found in the transformation folder at the point of running shell script. This zipped folder should be passed from the cmd line next to the `packages.zip`.

## Running the ETL job

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--packages 'com.somesparkjar.dependency:1.0.0' \
--py-files packages.zip, transformations.zip \
--files configs/etl_config.json \
jobs/etl_v1_job.py job_name
```

Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--packages 'com.somesparkjar.dependency:1.0.0,...'` - Maven coordinates for any JAR dependencies required by the job (e.g. JDBC driver for connecting to a relational database);
- `--files configs/etl_config.json` - the (optional) path to any config file that may be required by the ETL job;
- `--py-files packages.zip,transformations.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_v1_job.py job_name` - the Python module file containing the ETL job to execute along with the desired job name.

Full details of all possible options can be found [here](http://spark.apache.org/docs/latest/submitting-applications.html). Note, that we have left some options to be defined within the job (which is actually a Spark application) - e.g. `spark.cores.max` and `spark.executor.memory` are defined in the Python script as it is felt that the job should explicitly contain the requests for the required cluster resources.


## Automated Testing

Automated testing is extended to cover two different cases. 

One for the inherited employees data, where the expected results are stored within `tests/test_data/employees_report` and the original data within `tests/test_data/employees`. Updated transformations against the original project version contain two independent actions, first capitalizing all letters in the first name and the last name, and the second one, multiplying floor column with the steps value to get the actual number of steps.

Second one, retail data with courtesy of [source](https://github.com/databricks/Spark-The-Definitive-Guide/tree/master/data) taken from [source](http://archive.ics.uci.edu/ml/datasets/Online+Retail) limited to the first 15 days of december 2010. Applied transformations consist of firstly grouping the data by ‘Stock Code’ and ‘Country’ in order to output total items sold per Country and secondly filtering such data per country where country is received from the configuration file. Expected data and test data are located in the same place as the first example but in the respective folder `retail-data`

In order to run the test:
```bash
pipenv run python -m unittest tests/test_etl_v1_job.py
```


## Managing Project Dependencies using Pipenv

We use [pipenv](https://docs.pipenv.org) for managing project dependencies and Python environments (i.e. virtual environments). All direct packages dependencies (e.g. NumPy may be used in a User Defined Function), as well as all the packages used during development (e.g. PySpark, flake8 for code linting, IPython for interactive console sessions, etc.), are described in the `Pipfile`. Their **precise** downstream dependencies are described in `Pipfile.lock`.

For more details please reference the original version of the repository.