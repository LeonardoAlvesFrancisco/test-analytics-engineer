from data_profiling import ProfilingGenerator
from data_loader import DataFrameBuilder
from data_transformation import DataNormalizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

class DataCleaner:
        
    def __init__(self) -> None:
        self.coletas, self.products = DataFrameBuilder().raw_dataframe_builder()
        self.normalizer = DataNormalizer

    def clean_coleta(self):
        self.coletas = self.normalizer.round_column_values(self.coletas, "FinalPrice")
        self.coletas = self.normalizer.fill_missing_with_zero_spark(self.coletas, "FinalPrice")
        self.coletas = self.normalizer.convert_missing_to_null_spark(self.coletas, "Screenshot")
        self.coletas = self.normalizer.fill_missing_with_zero_spark(self.coletas, "RandomPrecosMissing")
        self.coletas = self.normalizer.convert_double_to_int(self.coletas, "RandomPrecosMissing")
        self.coletas = self.normalizer.round_column_values(self.coletas, "RandomPrecosDiscrepantesFator")
        self.coletas  = self.normalizer.transform_ones_to_zeros(self.coletas, "RandomPrecosNegativos")
        self.coletas = self.normalizer.transform_negatives_to_one(self.coletas,"RandomPrecosNegativos")
        return self.coletas
        
    def clean_produto(self):
        self.products = self.normalizer.remove_duplicates(self.products)
        self.products = self.normalizer.convert_missing_to_null_spark(self.products, "Brand")
        return self.products
