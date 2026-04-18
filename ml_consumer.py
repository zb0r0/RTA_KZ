from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://localhost:8001/score"

for message in consumer:
    tx = message.value
    
    try:
        # 1. Wyciągnięcie cech
        amount = tx.get("amount", 0)

        hour = tx.get("hour", 0)
        
        is_electronics = 1 if tx.get("category") == "elektronika" else 0
        
        tx_per_day = 5

        features = {
            "amount": amount,
            "hour": hour,
            "is_electronics": is_electronics,
            "tx_per_day": tx_per_day
        }

        # 2. Zapytanie do API
        response = requests.post(API_URL, json=features)
        result = response.json()

        # 3. Jeśli fraud → alert
        threshold = 0.9

        if result.get("fraud_probability", 0) > threshold:
            alert = {
                "timestamp": datetime.utcnow().isoformat(),
                "transaction": tx,
                "fraud_probability": result.get("fraud_probability")
            }
        
            alert_producer.send("alerts", alert)
            print("🚨 ALERT:", alert)
        else:
            print("OK:", result.get("fraud_probability"))

    except Exception as e:
        print("Błąd przetwarzania:", e)
