from data_transformation.data_cleaner import DataCleaner
from sklearn.linear_model import LogisticRegression
from sklearn.cluster import KMeans

import pandas as pd

class DataModeling:
    
    def __init__(self) -> None:
        self.coleta = DataCleaner().clean_coleta()
        self.products = DataCleaner().clean_produto()

    
    def logistic_regression(self):
        df = self.products.limit(1000).toPandas()
        df = df[['Customer', 'Department', 'Category', 'Brand', 'EAN', 'Product', 'Retailer', 'MasterKey_RetailerProduct']]

        # Codifique as colunas categóricas
        df = pd.get_dummies(df)

        # Determine o número ideal de clusters
        inertias = []
        for k in range(1, 5):
            kmeans = KMeans(n_clusters=k)
            kmeans.fit(df)
            inertias.append(kmeans.inertia_)

        # Plote a curva de codoide


        # Baseado na curva de codoide, o número ideal de clusters é 4
        kmeans = KMeans(n_clusters=4)
        kmeans.fit(df)
        labels = kmeans.labels_

        # Imprima os rótulos de cluster para cada linha
        print(labels)



        # Imprima a média de cada coluna para cada cluster
        print(kmeans.cluster_centers_)