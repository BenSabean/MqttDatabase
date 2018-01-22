from DataBase.DbManager import DbManager
from threading import Thread
from Queue import Queue
import threading
import paho.mqtt.client as mqtt #MQTT client
import time
import datetime
import json

class ModuleThread(Thread):
    def __init__ (self, myIndex, myInterval, mySensors):
        Thread.__init__(self)
        self.index = myIndex
        self.interval = myInterval
        self.sensors = mySensors

    def setInterval(self, myInterval):
        self.interval = myInterval

    def run(self):
        time.sleep(1)
        while True:
            print("Module ID:", self.index)
            print("Sleeping for " + `self.interval` + " minute(s)")
            time.sleep(60*self.interval)
            q.join()
            print("Attempting to log data...")
            print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            q.put(dict([('index', self.index), ('sensors', self.sensors)]))

with open('config.json') as json_data_file:
    data = json.load(json_data_file)

db = DbManager(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])

threads = []
q = Queue()

topic = "AERlab/WaterTanks/Tank1/Temperature/Data/+"
TDtopic = "AERlab/WaterTanks/Tank2/Temperature/Data/+" 
EEtopic = "Home/EnergyMonitor/EagleEye/Current/Data/+"
PVSvalues = [None] * 5
TDvalues = [None] * 2
EEvalues = [None] * 8

def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    if(message.topic[:23] == "AERlab/WaterTanks/Tank1"):
        PVSvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
    elif(message.topic[:27] == "Home/EnergyMonitor/EagleEye"):
        EEvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
        print("Eagle Eye", EEvalues)
        if(db.insertData(0, 8, ["NUll"] + EEvalues)):
            print("Inserted Data")
            EEvalues[0] = None
            EEvalues[1] = None
            EEvalues[2] = None
            EEvalues[3] = None
            EEvalues[4] = None
            EEvalues[5] = None
            EEvalues[6] = None
            EEvalues[7] = None
        else:
            print("Failed to insert data")
    else:
        TDvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))

def on_connect(client, userdata, flags, rc):
    
    if rc == 0:
        print("Connected to broker")
        global Connected                #Use global variable
        Connected = True                #Signal connection
    
    else:
        print("Connection failed")
        print("Attempting reconnect")
        connect(broker_address)

def connect():
    print("connecting to broker")
    client.username_pw_set(data["mqtt"]["user"], data["mqtt"]["passwd"])
    client.connect(data["mqtt"]["host"]) #connect to broker

print("creating new instance")
client = mqtt.Client("PI_DB") #create new instance
client.on_connect = on_connect        #attach function to callback
client.on_message = on_message        #attach function to callback

connect()

client.loop_start() #start the loop

print("Subscribing to topic",topic)
client.subscribe(topic)
print("Subscribing to topic",TDtopic)
client.subscribe(TDtopic)
print("Subscribing to topic",EEtopic)
client.subscribe(EEtopic)

print("Number of devices: " + `db.getDeviceCount()`)

for i in range(0, db.getDeviceCount()):
    print("Spawning thread " + `i+1`)
    thModule = ModuleThread(i+1, int(db.getSampleRate(i+1)), db.getSensorCount(i+1))
    thModule.daemon = True
    thModule.start()
    threads.append(thModule)

#print("Spawning thread 2")
#thModule = ModuleThread(2, int(db.getSampleRate(2)), 2)
#thModule.daemon = True
#thModule.start()
#threads.append(thModule)

data = []

try:
    while(1):
        # block main thread until queue is populated
        module = q.get()
        data = [PVSvalues, TDvalues, EEvalues]
        print("Writing values to database: " + `module["index"]`)
        print("Values: ", data[module["index"]-1])
        try:
            if(db.insertData(module["index"], module["sensors"], ["NUll"] + data[module["index"]-1])):
       	        print("Inserted Data")
            else:
                print("Failed to insert data")

            for i in range(len(data[module["index"]-1])):
                data[module["index"]-1][i] = None

            threads[module["index"]-1].setInterval(int(db.getSampleRate(module["index"])))

        except Exception as e:
            print(e)
        q.task_done()
except KeyboardInterrupt:
    print "\nexiting"
    client.disconnect()
    client.loop_stop()
