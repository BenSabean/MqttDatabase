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
    
    def run(self):
        time.sleep(1)
        while True:
            time.sleep(60*self.interval)
            q.join()
            print("Attempting to log data...")
            print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            #q.put([self.index, self.sensors])
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
    #print("message received " ,str(message.payload.decode("utf-8")))
    #print("message topic=",message.topic)
    #print("message qos=",message.qos)
    #print("message retain flag=",message.retain)
    if(message.topic[:23] == "AERlab/WaterTanks/Tank1"):
        PVSvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
    elif(message.topic[:27] == "Home/EnergyMonitor/EagleEye"):
        EEvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
        print("Eagle Eye", EEvalues)
        if(db.insertData(8, 8, ["NUll"] + EEvalues)):
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

print("Spawning thread 1")
thModule = ModuleThread(1, db.getSampleRate(1), 5)
thModule.daemon = True
thModule.start()
threads.append(thModule)

print("Spawning thread 2")
thModule = ModuleThread(2, db.getSampleRate(2), 2)
thModule.daemon = True
thModule.start()
threads.append(thModule)

data = []

try:
    while(1):
        #time.sleep(300) # wait
        module = q.get()
        data = [PVSvalues, TDvalues, EEvalues]
        print("Writing values to database: " + `module["index"]`)
        #print("PV Solar Boiler", PVSvalues) 
        try:
            if(db.insertData(module["index"], module["sensors"], ["NUll"] + data[module["index"]-1])):
       	        print("Inserted Data")
            else:
                print("Failed to insert data")
            
            #print("ThermoDynamics Tank", TDvalues)
            #if(db.insertData(7, 2, ["NUll"] + TDvalues)):
            #    print("Inserted Data")
            #else:
            #    print("Failed to insert data")

            #print("Eagle Eye", EEvalues)
            #if(db.insertData(8, 8, ["NUll"] + EEvalues)):
            #    print("Inserted Data")
            #else:
            #    print("Failed to insert data")
        except Exception as e:
            print(e)
        q.task_done()
except KeyboardInterrupt:
    print "\nexiting"
    client.disconnect()
    client.loop_stop()
