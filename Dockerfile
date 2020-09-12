FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

RUN pip install joblib scikit-learn

COPY /models/dt_4.joblib /models/
COPY /app /app
