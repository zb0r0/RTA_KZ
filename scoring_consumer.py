from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def score_transaction(tx):
    score = 0
    rules = []

    amount = tx['amount']
    category = tx['category']
    hour = datetime.fromisoformat(tx['timestamp']).hour

    if amount > 3000:
        score += 3
        rules.append('R1')

    if category == 'elektronika' and amount > 1500:
        score += 2
        rules.append('R2')

    if hour < 6:
        score += 2
        rules.append('R3')

    return score, rules


consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    tx = message.value

    score, rules = score_transaction(tx)

    if score >= 3:
        alert = {
            'tx_id': tx['tx_id'],
            'score': score,
            'rules': rules,
            'original': tx
        }

        alert_producer.send('alerts', value=alert)

        print(f"🚨 ALERT: {tx['tx_id']} | score={score} | rules={rules}")

    alert_producer.flush()
    
# TWÓJ KOD
# Dla każdej transakcji: scoruj, jeśli >= 3: wyślij do 'alerts' i wypisz ALERT
