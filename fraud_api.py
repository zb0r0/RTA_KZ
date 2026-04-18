from fastapi import FastAPI
from pydantic import BaseModel
import pickle, numpy as np

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    hour: int
    is_electronics: int
    tx_per_day: int


# Endpoint POST /score
@app.post("/score")
def score(tx: Transaction):
    # zamiana na array
    data = np.array([[ 
        tx.amount, 
        tx.hour, 
        tx.is_electronics, 
        tx.tx_per_day 
    ]])
    
    # predykcja
    pred = model.predict(data)[0]
    
    # prawdopodobieństwo klasy 1 (fraud)
    proba = model.predict_proba(data)[0][1]
    
    return {
        "is_fraud": bool(pred),
        "fraud_probability": float(proba)
    }

@app.get("/health")
def health():
    return {"status": "ok"}
