import os
from flask import g
from pyspark.sql import SparkSession


def get_spark_session():
    if 'spark' not in g:
        os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Nilesh\\Desktop\\xirr-calculator\\.venv\\Scripts\\python.exe'
        g.spark = SparkSession.builder \
            .appName("Log Analysis") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()

        g.spark.sparkContext.setLogLevel("WARN")

    return g.spark
