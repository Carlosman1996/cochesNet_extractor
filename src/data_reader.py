import pandas as pd
from src.repository import Repository

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

class DataReader:
    def __init__(self):
        pass

    def run(self):
        data_df = Repository.read_all_to_df()
        data_df = data_df[["PRICE", "WARRANTY_MONTHS", "IS_FINANCED", "IS_CERTIFIED", "IS_PROFESSIONAL", "HAS_URGE", "KM", "YEAR", "CC", "PROVINCE", "FUEL_TYPE"]]

        data_df_sorted = data_df.groupby("PROVINCE")["PRICE"].mean().round().sort_values(ascending=False)
        data_df_sorted = data_df_sorted.reset_index()
        print(data_df_sorted)

        # sns.pairplot(data_df)
        sns.barplot(x='PROVINCE', y='PRICE', data=data_df, order=data_df_sorted['PROVINCE'], estimator=np.mean)
        plt.show()


if __name__ == "__main__":
    data_reader_obj = DataReader()
    data_reader_obj.run()
