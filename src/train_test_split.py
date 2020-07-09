import pandas as pd
import numpy as np

df = pd.read_csv('../inputs/all_data.csv')
df['Reg. Date'] = pd.to_datetime(df['Reg. Date'], format = '%Y-%m-%d')
df = df.sort_values(by='Reg. Date', ascending=True) # train test split is done by time

df_train, df_test = np.split(df, [int(0.8*len(df))])

df_train.to_csv("../inputs/train.csv", index=False)
df_test.to_csv("../inputs/test.csv", index=False)
