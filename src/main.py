from data_profiling import ProfilingGenerator
from data_transformation.data_cleaner import DataCleaner
from data_modeling.data_modeling import  DataModeling
from data_loader import DataFrameBuilder
from data_transformation import DataNormalizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

if __name__ == "__main__":
    DataModeling().logistic_regression()