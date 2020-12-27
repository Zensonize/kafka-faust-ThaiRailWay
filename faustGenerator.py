import faust
import random
import json
import asyncio
from datetime import datetime

#python3 faustGenerator.py worker

f = open('data.json','r')
data = json.load(f)
f.close()

global generatedTrain
global finishedTrain
global trainNum

generatedTrain = []
finishedTrain = []
trainNum = 0
def timeEl():
    dt = datetime.now()
    ts = dt.strftime("[ %H:%M:%S.%f ]")
    return ts

def generateTrainID():
    global trainNum
    trainNum+=1
    return trainNum

def generateTime():
    return str(random.randint(0,23)) + ':' + str(random.randint(0,59))

def nextDepartTime(t):
    h,m = t.split(':')
    h = int(h)
    m = int(m) + 10
    if m >= 60 :
        h = h + 1
        m = m - 60

    if h == 24:
        h = 0

    return str(h) + ":" + str(m)

def nextArrivalTime(t):
    h,m = t.split(':')
    h = int(h) + 1

    if int(h) == 24:
        h = 0
    return str(h) + ":" + m

def generatePath(train):
    pathedTrain = train
    startStation = {
        'station': train['origin'],
        'arrival': '-',
        'departure': train['departure']
    }
    pathedTrain['path'].append(startStation)

    for i in range(random.randint(2,10)):
        stop = {
            'station': random.choice(data['provinceList']),
            'arrival': nextArrivalTime(pathedTrain['path'][-1]['departure']),
            'departure': nextDepartTime(nextArrivalTime(pathedTrain['path'][-1]['departure']))
        }
        pathedTrain['path'].append(stop)

    endStation = {
        'station': train['destination'],
        'arrival': nextArrivalTime(pathedTrain['path'][-1]['departure']),
        'departure': '-'
    }
    pathedTrain['path'].append(endStation)
    return pathedTrain

async def generateTrain():
    train = {
        'id': generateTrainID(),
        'type': random.choice(data['trainType']),
        'origin': random.choice(data['provinceList']),
        'destination': random.choice(data['provinceList']),
        'path': [],
        'departure': generateTime(),
        'arrival': '',
        'state': '0.0'
    }
    
    train = generatePath(train)
    # print('generated Train', json.dumps(train, indent=3, ensure_ascii=False))
    generatedTrain.append(train)
    return(train)

def nextTrainState(train):
    station, state = train['state'].split('.')
    # print('current state', train['state'], 'total station', len(train['path']))
    ret = {
        'id': train['id'],
        'state': "",
        'msg': ""
    }

    if int(station) == (len(train['path']) - 1) and state == "1":
        ret['state'] = station + ".4"
        ret['msg'] = "train {} arrived at the destination {} station".format(train['id'], train['destination'])

        return ret, True

    if state == "0":
        ret['state'] = station + ".1"
        ret['msg'] = "train {} arriving at the {} station".format(train['id'],train['path'][int(station)]['station'])

        return ret, False
    if state == "1":
        ret['state'] = station + ".2"
        ret['msg'] = "train {} boarding at the {} station".format(train['id'],train['path'][int(station)]['station'])

        return ret, False
    
    if state == "2":
        ret['state'] = station + ".3"
        ret['msg'] = "train {} departed the {} station".format(train['id'],train['path'][int(station)]['station'])

        return ret, False

    if state == "3":
        ret['state'] = str(int(station) + 1) + ".1"
        ret['msg'] = "train {} is in transit to the {} station".format(train['id'],train['path'][int(station)+1]['station'])

        return ret, False

async def updateTrainStatus():
    #pick a train to update status
    if(len(generatedTrain) != 0):
        train = random.randint(0,(len(generatedTrain)-1))
        nextState, isArrived = nextTrainState(generatedTrain[train])
        generatedTrain[train]['state'] = nextState['state']

        if isArrived:
            tr = generatedTrain.pop(train)
            finishedTrain.append(tr)
        
        return nextState
    
    return None

if __name__ == '__main__':
    """Simple Faust Producer"""

    # Create the Faust App
    app = faust.App('train_event_generator', broker='localhost:9092')
    train_topic = app.topic('new-schedule')
    train_status_topic = app.topic('train-status')

    # Send messages
    @app.timer(interval=10.0)
    async def send_new_train():
        print('sending new train')
        newTrain = await generateTrain()
        await train_topic.send(value=newTrain)

    @app.timer(interval=2.0)
    async def send_train_status():
        
        print('sending new train status')
        newStatus = await updateTrainStatus()
        if newStatus != None:
            await train_status_topic.send(value=newStatus)

    # Start the Faust App
    app.main()