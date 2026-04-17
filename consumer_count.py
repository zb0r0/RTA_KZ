from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    data = message.value
    store = data['store']
    amount = data['amount']

    # 1. zliczanie transakcji
    store_counts[store] += 1

    # 2. sumowanie kwot
    total_amount[store] += amount

    msg_count += 1

    # 3. co 10 wiadomości -> wypisz podsumowanie
    if msg_count % 10 == 0:
        print("\n=== PODSUMOWANIE ===")
        print(f"Przetworzono: {msg_count} wiadomości\n")

        for store in store_counts:
            print(f"{store}:")
            print(f"  liczba transakcji: {store_counts[store]}")
            print(f"  suma: {total_amount[store]:.2f} PLN")

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. store_counts[store] += 1
#   2. total_amount[store] += amount
#   3. Co 10 wiadomości: print tabela
