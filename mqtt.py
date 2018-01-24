from DataBase.DbManager import DbManager
from threading import Thread
from Queue import Queue
import threading
import paho.mqtt.client as mqtt #MQTT client
import time
import datetime
import json

# Thread to handle the sample time for each connected DAQ Module
class ModuleThread(Thread):
    # initialization routine
    def __init__ (self, myIndex, myInterval, mySensors):
        Thread.__init__(self)
        self.index = myIndex          # Device ID
        self.interval = myInterval    # Time ineterval (Min)
        self.sensors = mySensors      # Sensors

    # Set a new sample rate for the DAQ module
    def setInterval(self, myInterval):
        self.interval = myInterval

    # Sleep for designated Sample time then send identifying info to main
    # thread
    def run(self):
        time.sleep(1)
        while True:
            #print("Module ID:", self.index)
            #print("Sleeping for " + `self.interval` + " minute(s)")
            time.sleep(60*self.interval)
            q.join()
            print("Attempting to log data...")
            print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            q.put(dict([('index', self.index), ('sensors', self.sensors)]))

# Get info from config file
with open('config.json') as json_data_file:
    data = json.load(json_data_file)

# Create new instance of DbManager class
db = DbManager(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])

threads = []    # list of DAQ module threads
q = Queue()     # container for sending identifying info to main thread

# MQTT subscribing topics
topic = "AERlab/WaterTanks/Tank1/Temperature/Data/+"
TDtopic = "AERlab/WaterTanks/Tank2/Temperature/Data/+" 
EEtopic = "Home/EnergyMonitor/EagleEye/Current/Data/+"

# containers to hold data
PVSvalues = [None] * 5
TDvalues = [None] * 2
EEvalues = [None] * 8

# Function to be run everytime a message is recieved through MQTT
def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    if(message.topic[:23] == "AERlab/WaterTanks/Tank1"):    # PV solar data
        PVSvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
    elif(message.topic[:27] == "Home/EnergyMonitor/EagleEye"):   #Eagle Eye data
        EEvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
        print("Eagle Eye", EEvalues)
        if(db.insertData(0, 8, ["NUll"] + EEvalues)):
            print("Inserted Data")
            # Reset container's values
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
    else:     # Thermodynamics data
        TDvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))

# Function runs when connect function returns.
def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("Connected to broker")
        global Connected                # Use global variable.
        Connected = True                # Signal connection.

    else:   # Something went wrong.
        print("Connection failed")
        print("Attempting reconnect")
        connect(broker_address)

# Attempt to connect to the MQTT broker.
def connect():
    print("connecting to broker")
    client.username_pw_set(data["mqtt"]["user"], data["mqtt"]["passwd"])
    client.connect(data["mqtt"]["host"]) # Connect to broker.

print("creating new instance")
client = mqtt.Client("PI_DB")         # Create new instance.
client.on_connect = on_connect        # Attach function to callback.
client.on_message = on_message        # Attach function to callback.

connect()

client.loop_start() # Start the loop.

# Subscribe to required topics.
print("Subscribing to topic",topic)
client.subscribe(topic)
print("Subscribing to topic",TDtopic)
client.subscribe(TDtopic)
print("Subscribing to topic",EEtopic)
client.subscribe(EEtopic)

# Create threads to handle sample rates of DAQ modules.
for i in range(0, db.getDeviceCount()):
    print("Spawning thread " + `i+1`)
    thModule = ModuleThread(i+1, int(db.getSampleRate(i+1)), db.getSensorCount(i+1))
    thModule.daemon = True
    thModule.start()
    threads.append(thModule)

# List to hold data for all DAQ modules.
data = []
#for x in range(0, db.getDeviceCount()):
#    print("Device ID", x)
#    print("Num Sensors: ", `db.getSensorCount(x)`)
#    data.append([None] * db.getSensorCount(x))
try:
    while(1):
        # Block main thread until queue is populated.
        module = q.get()
        data = [PVSvalues, TDvalues, EEvalues]   # Update data list.
        print("Writing values to database: " + `module["index"]`)
        print("Values: ", data[module["index"]-1])
        try:
            # Attempt to insert data into database.
            if(db.insertData(module["index"], module["sensors"], ["NUll"] + data[module["index"]-1])):
       	        print("Inserted Data")
            else:
                # Something sent wrong. Most likely the data was not recieved in time
                # or some of the data was missing.
                print("Failed to insert data")

            # Reset data for the DAQ module that was just inserted (or failed).
            for i in range(len(data[module["index"]-1])):
                data[module["index"]-1][i] = None

            # Set DAQ module's sample rate to the sample rate currently in the database.
            threads[module["index"]-1].setInterval(int(db.getSampleRate(module["index"])))

        except Exception as e:
            print(e)
        q.task_done()     # Remove item from the queue.

# Allow program to disconnect gracefully when recieving a CTRL-C interrupt.
except KeyboardInterrupt:
    print "\nexiting"
    client.disconnect()
    client.loop_stop()
