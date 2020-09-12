import pandas as pd
from sklearn import model_selection

import pdb

if __name__ == "__main__":
    df = pd.read_csv("../inputs/centaline/train.csv")

    cols = ["HMA_Lat","HMA_Lng","blg_age","RFT_UPrice"]

    df = df[cols]
    #df["Y_Axis"] = df["Y_Axis"].str.extract(r'(\d*)')
    df = df[df['blg_age']>0]
    df = df.apply(pd.to_numeric)
    df = df.dropna()
    print(df.shape)
    #pdb.set_trace()

    df["kfold"] = -1
    
    df = df.sample(frac=1).reset_index(drop=True)

    kf = model_selection.KFold(n_splits=5)
    
    for fold, (trn_, val_) in enumerate(kf.split(X=df)):
        df.loc[val_, 'kfold'] = fold

    df.to_csv("../inputs/centaline/train_folds.csv", index=False)