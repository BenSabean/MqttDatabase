import MySQLdb
from DataEntry import DataEntry

class DbManager:
    def __init__(self, myHost, myUser, myPasswd, myDb):

        self.data=MySQLdb.connect(host=myHost,user=myUser,
        passwd=myPasswd,db=myDb)

        self.c=self.data.cursor()
        self.mydb = myDb
        self.db = DataEntry(self.data, self.c)
        self.deviceTable = "Devices"
        self.addressTable = "Addresses"

    def __getTables(self):
        try:
            self.c.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                           + "WHERE TABLE_TYPE = 'BASE TABLE' "
                           + "AND TABLE_SCHEMA='" + self.mydb + "'")
            return self.c.fetchall()
        except:
            pass

    def __decodeID(self, deviceID):
        try:
            self.c.execute("SELECT `Table Name` FROM `" + self.deviceTable + "` WHERE "
                           + "`Device ID` = " + `deviceID`)
            return self.c.fetchall()[0][0]
        except:
            pass

    def __getDevices(self):
        try:
            self.c.execute("SELECT `Table Name` FROM `" + self.deviceTable + "`")
            return self.c.fetchall()
        except:
            pass

    def getSensorCount(self, DeviceID):
        try:
            self.c.execute("SELECT Sensors FROM `" + self.deviceTable + "` WHERE "
                           + "`Device ID` = \"" + deviceID + "\"")
            return int(self.c.fetchall()[0][0])
        except:
            pass

    def __tableExists(self,tables,device):
        if(any(device in x for x in tables)):
            return True
        else:
            return False

    def __createTable(self,tableName, numSensors):

        query = "CREATE TABLE `" + tableName + "` (`TimeStamp` timestamp, "

        for i in range(0, numSensors):
            query += "`Sensor" + `(i+1)` + "` double,"
        query = query[:-1]
        query += ');'
        
        try:
            self.c.execute(query)
            return True
        except:
            return False

    def insertData(self, deviceID, numSensors, data):
        table = self.__decodeID(deviceID)
        if(not self.__tableExists(self.__getTables(), table)):
            print("Creating Table")
            if(not self.__createTable(table, numSensors)):
                print("Table Creation Failed")
        if(self.db.insertData(table, data)):
            return True
        else:
            return False

    def getSampleRate(self, deviceID):
        try:
            self.c.execute("SELECT `Time Interval (Min)` FROM `" + self.deviceTable + "` WHERE "
                           + "`Device ID` = " + `deviceID`)
            return self.c.fetchall()[0][0]
        except:
            pass

    def getDeviceCount(self):
        try:
             self.c.execute("SELECT COUNT(*) FROM `" + self.deviceTable + "` WHERE `Device ID` > 0")
             return int(self.c.fetchall()[0][0])
        except:
            pass
