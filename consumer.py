from kafka import KafkaConsumer
from database import salvar_transacao, criar_tabela
from collections import defaultdict, deque
from datetime import datetime, timedelta
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

# customer_id -> deque de (datetime, cidade) das últimas transações (janela de 10 min)
historico = defaultdict(deque)

def detectar_fraudes(transacao):
    fraudes = []
    cliente = transacao['customer_id']
    cidade =  transacao['city']
    agora = datetime.fromisoformat(transacao['timestamp'])
    hist = historico[cliente]

    # Descarta o que saiu da janela de 10 minutos
    while hist and agora - hist[0][0] > timedelta(minutes=10):
        hist.popleft()

    # Case Alto Valor
    if transacao['amount'] >= 10000:
        fraudes.append("ALTO_VALOR")

    ## Caso Tempo: 3 anteriores em < 60s + a atual = 4 transações
    ultimos_60s = [t for t, _ in hist if (agora - t).total_seconds() < 60]
    if len(ultimos_60s) >= 3:
        fraudes.append("TEMPO_60s")

    ## Case GEO: transação anterior (em até 10 min) em cidade diferente
    if any(c != cidade for _, c in hist):
        fraudes.append("GEO_10m")

    hist.append((agora, cidade))  # Atualiza histórico com a transação atual
    return ",".join(fraudes) if fraudes else None

print("Consumidor iniciado. Aguardando transações...")

try:
    criar_tabela()  # Cria a tabela no banco de dados, se não existir
    for mensagem in consumer:
        transacao = mensagem.value
        fraud_type = detectar_fraudes(transacao)  # Detecta possíveis fraudes

        salvar_transacao(transacao, fraud_type)  # Salva a transação no banco de dados

        if fraud_type:
            print(f"\n--- Fraude detectada: {fraud_type} ---")
        else:
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