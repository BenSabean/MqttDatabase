# -----------------------------------------------------------------
# deviceConfig.py
# -----------------------------------------------------------------
# This program finds the next available device ID from the MySQL
# database and writes that value to the console.
# This program is meant to be run by NodeRed when a DAQ module
# posts to the server asking for a new device ID. The response will
# be taken from the console to NodeRed where it will be sent to the
# DAQ module that requested an ID.

from DataBase.DbManager import DbManager
import json
import sys

# Get info from config file
with open('/home/aerlab/MqttDatabase/config.json') as json_data_file:
    data = json.load(json_data_file)

# Create new instance of DbManager class
db = DbManager(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])

if(len(sys.argv) == 1):
    print("Input error")
    sys.exit()

deviceId = db.getAllIds()

last = 0
newID = 0
for i in range(0, db.getDeviceCount()+1):
    if(not (deviceId[i][0] == last + 1) and not (last == 0)):
        #print(last+1)
        newID = last +1
    elif(i == db.getDeviceCount()):
        #print(i+1)
        newID = i+1

    last = deviceId[i][0]

print(newID)

if(db.createDevice(str(newID),sys.argv[1])):
    pass
else:
    print("ERROR device not created")

