import os
from pandas import read_csv

folder = os.listdir("data/")

class CsvConverter:
    def __init__(self) -> None:
        self.data_relative_path = "data"

    def convert_to_parquet(self):

        for file in folder:
            if not file.endswith(".csv"):
                continue
            file_path = os.path.join(self.data_relative_path, file)
            
            df = read_csv(file_path, encoding="latin1")

            parquet_file = os.path.splitext(file_path)[0] + ".parquet"
            df.to_parquet(parquet_file)
            
            os.remove(file_path)

        print("All files converted to parquet")