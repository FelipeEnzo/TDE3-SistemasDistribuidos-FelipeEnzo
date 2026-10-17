from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

mensagem = "Olá, Kafka!"

producer.send('tde-transacoes', value=mensagem.encode('utf-8'))

producer.flush()
producer.close()

print("Mensagem enviada com sucesso!")