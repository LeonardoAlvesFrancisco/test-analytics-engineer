from data_loader import DataFrameBuilder
from ydata_profiling import ProfileReport
import pandas as pd

class ProfilingGenerator:

    def generate_profiling(dataframe, sample_number, file_name):
       return  ProfileReport(
                        dataframe.limit(sample_number).toPandas(),
                        explorative=True,
                        dark_mode=True,
                    ).to_file(f"reports/profiling/{file_name}.html")
