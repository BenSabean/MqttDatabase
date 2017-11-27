from DataBase.DbManager import DbManager
from threading import Thread
import paho.mqtt.client as mqtt #MQTT client
import time
import datetime
import json

with open('config.json') as json_data_file:
    data = json.load(json_data_file)

db = DbManager(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])

topic = "AERlab/WaterTanks/Tank1/Temperature/Data/+"
TDtopic = "AERlab/WaterTanks/Tank2/Temperature/Data/+" 
PVSvalues = [None] * 5
TDvalues = [None] * 2

def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    if(message.topic[:23] == "AERlab/WaterTanks/Tank1"):
        PVSvalues[int(message.topic[-1:]) - 1] = str(message.payload.decode("utf-8"))
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
client.subscribe(TDtopic)

try:
    while(1):
        time.sleep(300) # wait
        print("Writing values to database: ")
        print("PV Solar Boiler", PVSvalues) 
        try:
            if(db.insertData(6, 5, ["NUll"] + PVSvalues)):
       	        print("Inserted Data")
            else:
                print("Failed to insert data")
            
            print("ThermoDynamics Tank", TDvalues)
            if(db.insertData(7, 2, ["NUll"] + TDvalues)):
                print("Inserted Data")
            else:
                print("Failed to insert data")

        except Exception as e:
            print(e)
except KeyboardInterrupt:
    print "\nexiting"
    client.disconnect()
    client.loop_stop()
