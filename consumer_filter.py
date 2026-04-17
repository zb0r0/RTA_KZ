from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='filter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# TWÓJ KOD # Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT
for message in consumer:
    tx = message.value

    if tx['amount'] > 1000:
        print(f"ALERT: Transakcja {tx['tx_id']} ma kwotę {tx['amount']} PLN, co jest powyżej 1000 PLN.")
