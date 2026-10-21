import sqlite3
import os 

CAMINHO_BANCO = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'transacoes.db'
)
def conectar():
    conexao = sqlite3.connect(CAMINHO_BANCO)
    return conexao

def criar_tabela():
    conexao = conectar()
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transacoes (
            transaction_id TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            city TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            fraud_type TEXT
        )
    ''')
    conexao.commit()
    conexao.close()


def salvar_transacao(transacao, fraud_type=None):
    conexao = conectar()
    cursor = conexao.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO transacoes (transaction_id, customer_id, amount, city, timestamp, fraud_type)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (transacao['transaction_id'], transacao['customer_id'], transacao['amount'], transacao['city'], transacao['timestamp'], fraud_type))
    conexao.commit()
    conexao.close()

def listar_transacoes():
    conexao = conectar()
    cursor = conexao.cursor()
    cursor.execute('SELECT * FROM transacoes')
    transacoes = cursor.fetchall()

    for transacao in transacoes:
        print(f"ID: {transacao[0]}, Cliente: {transacao[1]}, Valor: R$ {transacao[2]:.2f}, Cidade: {transacao[3]}, Data/Hora: {transacao[4]}, Tipo de Fraude: {transacao[5]}")
    conexao.close()

if __name__ == "__main__":
    criar_tabela()

    transacao_teste = {
        "transaction_id": "teste-001",
        "customer_id": 1,
        "amount": 150.00,
        "city": "Barbalha",
        "timestamp": "2026-10-20T19:00:00+00:00",
        "fraud_type": None
    }

    salvar_transacao(transacao_teste)
    print("Tabela 'transacoes' criada com sucesso.")