"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""

import sys
import unittest

import json

from pyspark.sql.functions import mean

from dependencies.spark import start_spark, transform_data
from jobs.etl_v1_job import transform_data, extract_data, load_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        #self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = start_spark(app_name='retail_data_test_app')
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_retail_data(self):
        with open('configs/etl_v1_retail_data_test_config.json') as config_file:
            self.config = json.loads(config_file.read())
        df = extract_data(self.spark, self.config['data_source'])

        transformed_data = transform_data(df, self.config['transformations'])

        expected_data = (
            self.spark
            .read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(self.test_data_path + 'retail-data_report/*.csv'))

        expected_cols = len(expected_data.columns)
        transformed_cols = len(transformed_data.columns)

        expected_rows = expected_data.count()
        transformed_rows = transformed_data.count()

        expected_avg_total_items = (
            expected_data
            .agg(mean('Total items').alias('avg_total_items'))
            .collect()[0]
            ['avg_total_items'])

        transformed_avg_total_items = (
            transformed_data
            .agg(mean('Total items').alias('avg_total_items'))
            .collect()[0]
            ['avg_total_items'])

        self.assertEqual(expected_cols, transformed_cols)
        self.assertEqual(expected_rows, transformed_rows)
        self.assertEqual(expected_avg_total_items, transformed_avg_total_items)
        self.assertTrue([col in expected_data.columns
                         for col in transformed_data.columns])



    def test_transform_employee_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        with open('configs/etl_v1_employees_data_config.json') as config_file:
            self.config = json.loads(config_file.read())

        # assemble
        df = extract_data(self.spark, self.config['data_source'])

        expected_data = (
            self.spark
            .read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(self.test_data_path + 'employees_report/*.csv'))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        expected_avg_steps = (
            expected_data
            .agg(mean('steps').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # act
        transformed_data = transform_data(df, self.config['transformations'])

        cols = len(transformed_data.columns)
        rows = transformed_data.count()
        avg_steps = (
            transformed_data
            .agg(mean('steps').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns
                         for col in transformed_data.columns])


if __name__ == '__main__':
    unittest.main()
