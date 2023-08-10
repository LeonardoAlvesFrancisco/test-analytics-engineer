from pyspark.sql.functions import col, when, round
from numpy import nan
class DataNormalizer:
    
    def fill_missing_with_zero_spark(dataframe, column_name):
        return dataframe.withColumn(column_name, when(col(column_name).isNull(), 0).otherwise(col(column_name)))
        
    def convert_missing_to_null_spark(dataframe, column_name):
        return dataframe.withColumn(column_name, when(col(column_name).isNull(), None).otherwise(col(column_name)))
    
    def convert_missing_to_nan(dataframe, column_name):
        return dataframe.withColumn(column_name, when(col(column_name).isNull(), nan).otherwise(col(column_name)))

    def convert_negative_to_positive_spark(dataframe, column_name):
        return dataframe.withColumn(column_name, when(col(column_name) < 0, -col(column_name)).otherwise(col(column_name)))
    
    def convert_double_to_int(dataframe, column_name):
        return dataframe.withColumn(column_name, col(column_name).cast("int"))
    
    def remove_duplicates(dataframe):
        return dataframe.dropDuplicates()
    
    def round_column_values(dataframe, column_name):
        return dataframe.withColumn(column_name, round(col(column_name), 2))
    
    def transform_ones_to_zeros(dataframe, column_name):
        return dataframe.withColumn(column_name, when(col(column_name) == 1, 0).otherwise(col(column_name)))
    
    def transform_negatives_to_one(dataframe, column_name):
        return dataframe.withColumn(column_name, when(col(column_name) < 0, 1).otherwise(col(column_name)))