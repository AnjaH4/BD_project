from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import json

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

@app.route('/')
def index():
    return render_template('index.html')

def kafka_listener():
    consumer = KafkaConsumer(
        'rolling_avg',
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        data = message.value
        print('Kafka message:', data)
        try:
            socketio.emit('newdata', data)  # Removed broadcast=True
        except Exception as e:
            print(f'Error emitting data: {e}')



@socketio.on('connect')
def connect():
    print('Client connected')
    emit('response', {'data': 'Connected'})

if __name__ == '__main__':
    from threading import Thread
    thread = Thread(target=kafka_listener)
    thread.start()
    socketio.run(app, debug=True)
