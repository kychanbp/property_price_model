from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from joblib import load
from pydantic import BaseModel

clf = load('../models/dt_4.joblib')

def get_prediction(HMA_Lat,HMA_Lng,blg_age):
    x = [[HMA_Lat,HMA_Lng, blg_age,0]]

    y = clf.predict(x)[0]

    return {'prediction':y}

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://propertypricemodelapp-env.eba-57bfj6kd.ap-southeast-1.elasticbeanstalk.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ModelParams(BaseModel):
    HMA_lat:float
    HMA_Lng:float
    blg_age:float

@app.post("/predict")
def predict(params: ModelParams):
    
    pred = get_prediction(params.HMA_lat, params.HMA_Lng, params.blg_age)
    
    return pred