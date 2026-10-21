from kafka import KafkaConsumer
from database import salvar_transacao, criar_tabela
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
    criar_tabela()  # Cria a tabela no banco de dados, se não existir
    for mensagem in consumer:
        transacao = mensagem.value
        fraud_type = None  # Inicialmente, não há tipo de fraude

        if transacao['amount'] >= 10000:
            fraud_type = "ALTO_VALOR"
            print("\n--- ALERTA DE FRAUDE --- ALTO VALOR !!!")
            print("Detalhes da transação:", transacao)
        salvar_transacao(transacao, fraud_type)  # Salva a transação no banco de dados

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

## python -c "import database; database.listar_transacoes()"