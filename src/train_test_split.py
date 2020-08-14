import argparse
import pandas as pd
import numpy as np

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dataset",
        type=str
    )

    args = parser.parse_args()
    dataset = args.dataset

    if dataset == "original":
        df = pd.read_csv('../inputs/original/all_data.csv')
        df['Reg. Date'] = pd.to_datetime(df['Reg. Date'], format = '%Y-%m-%d')
        df = df.sort_values(by='Reg. Date', ascending=True) # train test split is done by time

        df_train, df_test = np.split(df, [int(0.8*len(df))])

        df_train.to_csv("../inputs/original/train.csv", index=False)
        df_test.to_csv("../inputs/original/test.csv", index=False)

    elif dataset == "centaline":
        df_transactions = pd.read_csv('../staging_area/CentalineTransactionsItem.csv')
        df_transactions_detail = pd.read_csv('../staging_area/CentalineTransactionsDetailItem.csv')
        df_building_info = pd.read_csv('../staging_area/CentalineBuildingInfo.csv')

        df = pd.merge(df_transactions, df_transactions_detail, left_on='TransactionID', right_on='ID', how='left')
        df = pd.merge(df, df_building_info, left_on=['Cblgcode','Cestcode'], right_on=['cblgcode','cestcode'], how='left')

        df_train, df_test = np.split(df, [int(0.8*len(df))])

        df_train.to_csv("../inputs/centaline/train.csv")
        df_test.to_csv("../inputs/centaline/test.csv")