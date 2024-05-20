from flask import Flask, request
from kafka import KafkaProducer

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:9092')

@app.route('/add_data', methods=['POST'])
def add_data():
    data = request.get_json()
    producer.send('test-topic', value=str(data).encode())
    return 'Data added to Kafka topic: test-topic'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')