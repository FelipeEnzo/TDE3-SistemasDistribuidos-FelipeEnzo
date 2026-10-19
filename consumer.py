from kafka import KafkaConsumer
import json 

consumer = KafkaConsumer(
    'tde-transacoes',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda mensagem: json.loads(
        mensagem.decode('utf-8')
    )
)

print("Consumidor iniciado. Aguardando tramsações...")

try:
    for mensagem in consumer:
        transacao = mensagem.value

        print("\n--- Transação recebida ---")
        print(f"ID: {transacao['transaction_id']}")
        print(f"Cliente: {transacao['customer_id']}")
        print(f"Valor: R$ {transacao['amount']:.2f}")
        print(f"Cidade: {transacao['city']}")
        print(f"Data/Hora: {transacao['timestamp']}")

except KeyboardInterrupt:
    print("\nConsumidor encerrado.")
finally:
    consumer.close()
    print("Conexão com o Kafka encerrada.")