from DataBase.DbManager import DbManager
from threading import Thread
from Queue import Queue
import threading
import paho.mqtt.client as mqtt #MQTT client
import time
import datetime
import json
import logging
import sys

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
        self.tmp = self.interval
        try:
            self.interval = myInterval
        except Exception as e:
            self.interval = self.tmp
            logging.info("Threading error: Setting sample rate.")
            logging.debug(str(e) + "\n")

    # Sleep for designated Sample time then send identifying info to main
    # thread
    def run(self):
        time.sleep(1)
        while True:
            try:
                print("Sleeping for " + `self.interval` + " minute(s)")
                time.sleep(60*self.interval)
                print("Attempting to log data...")
                print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                lock.acquire()
                q.put(dict([('index', self.index), ('sensors', self.sensors)]))
                lock.release()
            except Exception as e:
                logging.info("Threading Error: Main routine")
                logging.debug(str(e) + "\n")

# Get info from config file
with open('config.json') as json_data_file:
    data = json.load(json_data_file)

# Create error log file
logging.basicConfig(filename='error.log',format='%(asctime)s %(message)s', level=logging.DEBUG)

# Create new instance of DbManager class
db = DbManager(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])

threads = []    # list of DAQ module threads
q = Queue()     # container for sending identifying info to main thread
lock = threading.Lock()     # Lock for synchronizing threads

# Function to be run everytime a message is recieved through MQTT
def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    #print("message topic=",message.topic)
    #print("message qos=",message.qos)
    #print("message retain flag=",message.retain)
    try:
        data[int(message.topic[0])][int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
    except Exception as e:
        logging.info("Error storing data for device " + str(message.topic[0]))
        logging.debug(str(e) + "\n")
    print("Device ID " + str(message.topic[0]) + " : ", data[int(message.topic[0])])

    if(message.topic[:7] == topic[0][:-1]):   #Eagle Eye data
        print("Eagle Eye: ", data[0])
        if(db.insertData(0, 8, ["NUll"] + data[0])):
            print("Inserted Data")
            # Reset container's values
            data[0][0] = None
            data[0][1] = None
            data[0][2] = None
            data[0][3] = None
            data[0][4] = None
            data[0][5] = None
            data[0][6] = None
            data[0][7] = None
        else:
            print("Failed to insert data")
            #logging.info("Could not insert Eagle Eye data into database.\n")

# Function runs when connect function returns.
def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("Connected to broker")
        global Connected                # Use global variable.
        Connected = True                # Signal connection.

    else:   # Something went wrong.
        print("Connection failed")
        logging.info("Could not connect to MQTT broker.\n")
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

# MQTT subscribing topics
topic = []
for x in range(0, db.getDeviceCount()+1):
    try:
        topic.append(`x` + "/Data/+")
        # Subscribe to required topics.
        print("Subscribing to topic",topic[x])
        client.subscribe(topic[x])
    except Exception as e:
        logging.info("Error creating topic with id " + `x+1` + ".")
        logging.debug(str(e) + "\n")
        print("Could not create topic")
        sys.exit()

# Create threads to handle sample rates of DAQ modules.
for i in range(0, db.getDeviceCount()):
    print("Spawning thread " + `i+1`)
    try:
        thModule = ModuleThread(i+1, int(db.getSampleRate(i+1)), db.getSensorCount(i+1))
        thModule.daemon = True
        thModule.start()
        threads.append(thModule)
    except Exception as e:
        logging.info("Error while spawning threads with id " + `i+1` + ".")
        logging.debug(str(e) + "\n")
        print("Could not create threads")
        sys.exit()

# List to hold data for all DAQ modules.
data = []
for x in range(0, db.getDeviceCount()+1):
    try:
        print("Device ID", x)
        print("Num Sensors: ", `db.getSensorCount(x)`)
        data.append([None] * db.getSensorCount(x))
        print("container", data[x])
    except Exception as e:
        logging.info("Error while creating data container for device id " + `x` + ".")
        logging.debug(str(e) + "\n")
        print("Could not create data containers")
        sys.exit()
try:
    while(1):
        # Block main thread until queue is populated.
        module = q.get()
        #data = [PVSvalues, TDvalues, EEvalues]   # Update data list.
        print("Writing values to database: " + `module["index"]`)
        print("Values: ", data[int(module["index"])])
        try:
            # Attempt to insert data into database.
            if(db.insertData(module["index"], module["sensors"], ["NUll"] + data[module["index"]])):
       	        print("Inserted Data")
            else:
                # Something sent wrong. Most likely the data was not recieved in time
                # or some of the data was missing.
                print("Failed to insert data")
        except Exception as e:
                logging.info("Error while inserting data.")
                logging.debug(str(e) + "\n")


        # Reset data for the DAQ module that was just inserted (or failed).
        for i in range(len(data[module["index"]])):
            data[module["index"]][i] = None

        # Set DAQ module's sample rate to the sample rate currently in the database.
        try:
            threads[module["index"]-1].setInterval(int(db.getSampleRate(module["index"])))
        except Exception as e:
            logging.info("Error sending sample rate to thread.")
            logging.debug(str(e) + "\n")

        q.task_done()     # Remove item from the queue.

# Allow program to disconnect gracefully when recieving a CTRL-C interrupt.
#except KeyboardInterrupt:
except Exeption as e:
    logging.info("Fatal Error caused program to close.")
    logging.debug(str(e) + "\n")
    print "\nexiting"
    client.disconnect()
    client.loop_stop()
