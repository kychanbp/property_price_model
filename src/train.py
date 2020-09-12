import pandas as pd
import math
from sklearn import metrics
from sklearn.neighbors import KNeighborsRegressor

import joblib

def run(fold):
    df = pd.read_csv("../inputs/centaline/train_folds.csv")

    df_train = df[df.kfold != fold].reset_index(drop=True)
    df_valid = df[df.kfold == fold].reset_index(drop=True)

    x_train = df_train.drop('RFT_UPrice',axis=1).values
    y_train = df_train['RFT_UPrice'].values


    x_valid = df_valid.drop('RFT_UPrice', axis=1).values
    y_valid = df_valid['RFT_UPrice'].values

    neigh = KNeighborsRegressor(n_neighbors=3)
    neigh.fit(x_train, y_train)

    preds = neigh.predict(x_valid)

    error = math.sqrt(metrics.mean_squared_log_error(y_valid, preds))
    print(f"Fold={fold}, RMSLE={error}")

    joblib.dump(neigh, f"../models/dt_{fold}.bin")

if __name__ == "__main__":
    run(fold=0)
    run(fold=1)
    run(fold=2)
    run(fold=3)
    run(fold=4)

