import json
import time
from kafka import KafkaProducer
from glob import glob
import csv


def main():
    Producer=KafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'tickets'
    
    filepaths = glob('/home/anjah/Documents/mag/BD/project/BD_project/data/augmented/augmented/*.csv') #change this path
    print(len(filepaths))
    #sort the filepaths
    filepaths.sort()
    
    for file in filepaths:
        i = 0
        with open(file, 'r') as f:
            for line in f:
                if i == 0:
                    header = line.split(',')
                    i += 1
                reader = csv.reader(f)
                #header = next(reader)

                for row in reader:
                    data = {header[j]: row[j] for j in range(len(header))}
                    Producer.send(topic, value=data)
                    print('sent to kafka', data)
                    time.sleep(0.005)

                Producer.flush()
    Producer.close()

if __name__ == '__main__':
    main()
