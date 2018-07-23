import paho.mqtt.client as mqtt #import the client
from DataBase.DbManager import DbManager
import time
import json

def on_message(client, userdata, message):
    #print("message received " ,str(message.payload.decode("utf-8")))
    #print("message topic=",message.topic)
    #print("message qos=",message.qos)
    #print("message retain flag=",message.retain)
    try:
        mac = str(message.topic).split('/')[2]
        #print(mac)
        sensor = db.getTTSensor("Think Tank Meta Test", mac)
        print(sensor)
        data = str(message.payload.decode("utf-8")).split(':')[0]
        #print(data)
        client.publish("8/Data/Sensor" + str(sensor), data)
    except Exception as e:
        print(e)

def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("Connected to broker")
        global Connected                #Use global variable
        Connected = True                #Signal connection

    else:
        print("Connection failed")
        print("Attempting reconnect")
        connect(broker_address)

def connect(broker_address):
    print("connecting to broker")
    client.connect(broker_address) #connect to broker

def on_disconnect(client, userdata, rc):   #Not Working
    print("Client Disconnected")
    if(isRunning == True):
        print("Attempting reconnect")
        client.reconnect()

with open('config.json') as json_data_file:
    data= json.load(json_data_file)

db = DbManager(data["remap-mysql"]["host"], data["remap-mysql"]["user"],
               data["remap-mysql"]["passwd"], data["remap-mysql"]["db"])

broker_address="192.168.2.151"
topic = "8/RawData/+"
isRunning = True

print("creating new instance")
client = mqtt.Client("Python1") #create new instance
client.on_connect = on_connect        #attach function to callback
client.on_disconnect = on_disconnect  #attach function to callback
client.on_message = on_message        #attach function to callback

client.username_pw_set("aerlab", "server")
connect(broker_address)

client.loop_start() #start the loop
print("Subscribing to topic",topic)
client.subscribe(topic)

try:
    while True:
        #print("Publishing message to topic",topic)
        #client.publish(topic,"TEST")
        #print("DB:")
        #print(db.getTTSensor("Think Tank Meta Test","28AB9EFE080000EE"))
        time.sleep(1) # wait
except KeyboardInterrupt:
    print "exiting"
    isRunning = False
    client.disconnect()
    client.loop_stop()
