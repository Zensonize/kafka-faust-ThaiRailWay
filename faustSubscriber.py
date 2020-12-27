import faust
import random
import json
import asyncio
from datetime import datetime

# faust -A faustSubscriber worker -l info --web-port=6067
# """Simple Faust Subscriber"""

# Create the Faust App
app = faust.App('train_event_subscriber', broker='localhost:9092',topic_partitions=4)
train_topic = app.topic('new-schedule')
train_status_topic = app.topic('train-status')

# subscribe to new train
@app.agent(train_topic)
async def recevice_new_train(trains):
    async for train in trains:
        print('received new train schedule', train)

# subscribe to train event
@app.agent(train_status_topic)
async def recevice_new_trainEvent(events):
    async for event in events:
        print('received new train event', event)

# Start the Faust App
# app.main()