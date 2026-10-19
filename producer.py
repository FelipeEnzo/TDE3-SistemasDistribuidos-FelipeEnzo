from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, timezone

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda dados: json.dumps(dados).encode('utf-8'))

cidades = ['Barbalha', "Crato", "Juazeiro do Norte", "Fortaleza"]

print("Iniciando envio de mensagens para o Kafka...")

try:
    while True:
        transacao = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": random.randint(1, 10),
            "amount": round(random.uniform(10, 15000), 2),
            "city": random.choice(cidades),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        producer.send("tde-transacoes", transacao)
        producer.flush()

        print("Transação enviada:", transacao)

        time.sleep(3)

except KeyboardInterrupt:
    print("Envio de mensagens interrompido pelo usuário.")

finally:
    producer.close()
    print("Conexão com o Kafka encerrada.")

## docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic tde-transacoes --bootstrap-server localhost:9092