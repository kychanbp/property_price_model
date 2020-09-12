from fastapi import FastAPI
from joblib import load
from pydantic import BaseModel

clf = load('../models/dt_4.joblib')

def get_prediction(HMA_Lat,HMA_Lng,blg_age):
    x = [[HMA_Lat,HMA_Lng, blg_age,0]]

    y = clf.predict(x)[0]

    return {'prediction':y}

app = FastAPI()

class ModelParams(BaseModel):
    HMA_lat:float
    HMA_Lng:float
    blg_age:float

@app.post("/predict")
def predict(params: ModelParams):
    
    pred = get_prediction(params.HMA_lat, params.HMA_Lng, params.blg_age)
    
    return pred