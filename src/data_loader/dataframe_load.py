from pyspark.sql import SparkSession


class DataFrameBuilder:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.master('local[*]').appName('RawDataFrameBuilder').getOrCreate()

    def raw_dataframe_builder(self) -> object:
        return self.spark.read.option("inferSchema", True).parquet("data/Coletas.parquet"), self.spark.read.option("inferSchema", True).parquet("data/ProdutosVarejos.parquet")