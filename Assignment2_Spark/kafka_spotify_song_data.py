import json
import asyncio
import csv
import random
import pytz
import signal
from aiokafka import AIOKafkaProducer
from datetime import datetime, timedelta
from faker import Faker


def load_songs():
    with open('spotify-songs.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row['name'] for row in reader]

fake = Faker()

songs = load_songs()

topic = 'songs_stream'

def serializer(value):
    return json.dumps(value).encode()

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=serializer,
        compression_type="gzip")

    await producer.start()
    
    file = open('spotify-song-data.txt', 'a')
        
    names = [fake.name() for _ in range(10)] + ["Vasilis Drouzas"]      #List of names, including my own
    
    async def send_song():
        id = 0
        while True:
            
            person = random.choice(names)                    #pick a random name
            song = random.choice(songs)                      #pick a random song
            id = id + 1                             
            
            '''Code to adjust to Greek timezone, for more info 
                check the pytz library: https://pypi.org/project/pytz/
            '''
                
            gmt2_timezone = pytz.timezone('Europe/Bucharest')  # Adjust timezone to the Greek one (GMT +2)
            gmt2_time = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(gmt2_timezone)  #replace timezone

            current_time = gmt2_time.strftime('%Y-%m-%d %H:%M:%S')              #get the current time
            
            
            data = {"id":id, "person": person, "song": song, "time": current_time}
            print(data)
                
            file.write(json.dumps(data) + '\n')               #Write the data to the file
           
            await producer.send(topic, data)
            
            
            await asyncio.sleep(random.randint(1, 60))           #await a random time (up to a minute) before sending the next song

    task = asyncio.ensure_future(send_song())        #start sending songs

    def signal_handler(sig, frame):
        '''Function to handle user-requested 
           termination of the program (Ctrl + C)'''
        
        task.cancel()
        print("Producer stopped. Terminating program...")
    
    signal.signal(signal.SIGINT, signal_handler)

    try:
        await task
    except asyncio.CancelledError:
        pass

    file.close()             #close file

    await producer.stop()

loop = asyncio.get_event_loop()
result = loop.run_until_complete(produce())
