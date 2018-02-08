from DataBase.DbManager import DbManager
from DataBase.DataEntry import DataEntry
import MySQLdb
import json

# Get info from config file
with open('/home/aerlab/MqttDatabase/config.json') as json_data_file:
    data = json.load(json_data_file)

# Create new instance of DbManager class
db = DbManager(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])

sql = MySQLdb.connect(data["mysql"]["host"], data["mysql"]["user"],
               data["mysql"]["passwd"], data["mysql"]["db"])
c = sql.cursor()
data = DataEntry(sql, c)

startDate = "2018-02-02"
endDate = "2018-02-03"
currentData = db.getIntervalData("Eagle Eye", startDate, endDate)

dataSize = 0
sum = [0] *  db.getSensorCount(0)
avgCurrent = [None] *  db.getSensorCount(0)
avgPwr = avgCurrent
energy = avgCurrent

print("sum: ", sum)
print("Average current: ", avgCurrent)
print("Average Power: ", avgPwr)
print("Energy Used: ", energy)

for i in currentData:
    for x in range(0, db.getSensorCount(0)):
        sum[x] += i[x+1]
    dataSize += 1

print("Data Size: ", dataSize)

for i in range(0, len(sum)):
    avgCurrent[i] = sum[i]/dataSize
    print("avg current for sensor " + `i + 1` + " : ", avgCurrent[i])

totalPwr = 0.0

for i in range(0, len(sum)):
    avgPwr[i] = str(avgCurrent[i] * 120)
    print("Average power (W) :", avgPwr[i])
    totalPwr += float(avgPwr[i])

if(data.insertData("Eagle Eye Average Power", ["\"" + startDate + "\""] + avgPwr + [str(totalPwr)])):
    print("Inserted Data")
else:
    # Something went wrong. Most likely some of the data was missing.
    print("Failed to insert data")
hours = 0
min = 0
sec = 0
startTime = str(currentData[0][0]).split(" ")[1]
endTime = str(currentData[dataSize-1][0]).split(" ")[1]

print(startTime)
print(endTime)

h1 = int(startTime.split(":")[0])
m1 = int(startTime.split(":")[1])
s1 = int(startTime.split(":")[2])

h2 = int(endTime.split(":")[0])
m2 = int(endTime.split(":")[1])
s2 = int(endTime.split(":")[2])

hours = h2 - h1
sec = ((m2*60)+ s2)  - ((m1*60) + s1)

playTime = hours + float(sec)/3600
totalEnergy = 0.0
print("Play time (hours):", playTime)

for i in range(0,len(sum)):
    energy[i] = str(float(avgPwr[i])* playTime)
    print("Energy used for on Sensor " + `i + 1` + " (W/h): ", energy[i])
    totalEnergy += float(energy[i])

#if(data.insertData("Eagle Eye Average Power", ["\"" + startDate + "\""] + avgPwr + [str(totalPwr)])):
#    print("Inserted Data")
#else:
#    # Something went wrong. Most likely some of the data was missing.
#    print("Failed to insert data")


if(data.insertData("Eagle Eye Power Usage", ["\"" + startDate + "\""] + energy + [str(totalEnergy)])):
    print("Inserted Data")
else:
    # Something went wrong. Most likely some of the data was missing.
    print("Failed to insert data")


