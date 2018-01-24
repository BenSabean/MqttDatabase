import MySQLdb
from DataEntry import DataEntry

# Class to create, insert and manage tables in a database. Acts as a wrapper
# for the DataEntry class adding aditional functionality to retrieve number of
# sensors and sample rate for DAQ Modules.
class DbManager:
    def __init__(self, myHost, myUser, myPasswd, myDb):

        # Connect to MySQL batabase.
        self.data=MySQLdb.connect(host=myHost,user=myUser,
        passwd=myPasswd,db=myDb)

        self.c=self.data.cursor()
        self.mydb = myDb
        self.db = DataEntry(self.data, self.c)
        self.deviceTable = "Devices"     # Table of all DAQ modules
        self.addressTable = "Addresses"

    # Find all tables for DAQ modules.
    # Returns list of tables.
    def __getTables(self):
        try:
            self.c.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                           + "WHERE TABLE_TYPE = 'BASE TABLE' "
                           + "AND TABLE_SCHEMA='" + self.mydb + "'")
            return self.c.fetchall()
        except:
            pass

    # Find device name from device ID.
    # Param deciceID The ID given to the DAQ module as defined by
    #       the `Device ID` column in the Devices table.
    # Returns the name of the device.
    def __decodeID(self, deviceID):
        try:
            self.c.execute("SELECT `Table Name` FROM `" + self.deviceTable + "` WHERE "
                           + "`Device ID` = " + `deviceID`)
            return self.c.fetchall()[0][0]
        except:
            pass

    # Find all registered devices.
    # Returns a list of all registered devices.
    def __getDevices(self):
        try:
            self.c.execute("SELECT `Table Name` FROM `" + self.deviceTable + "`")
            return self.c.fetchall()
        except:
            pass

    # Find the number of sensors that is supposed to be attached to a device.
    # Param DeviceID The ID given to the DAQ module.
    # Returns The number of sensors registered to a device.
    def getSensorCount(self, DeviceID):
        try:
            print("Device ID", DeviceID) 
            self.c.execute("SELECT `Sensors` FROM `" + self.deviceTable + "` WHERE "
                           + "`Device ID` = " + `DeviceID`)
            return int(self.c.fetchall()[0][0])
        except:
            pass

    # Find if a table exists for the geven DAQ module.
    # Param table A lists of all tables for DAQ modules.
    # Param device The mane of the device to see whether a table
    #      already exists for it.
    # Returns True if the table already exists, False otherwise.
    def __tableExists(self,tables,device):
        if(any(device in x for x in tables)):
            return True
        else:
            return False

    # Create a new table for the given DAQ module
    # Param tablename The device's name to be used as a table name.
    # Param numSensors The number of sensors registered to the DAQ module
    #     Sensors will be named in the format: Sensor1, Sensor2, ... , Sensor N
    # Returns True if the table was created successfully, False otherwise.
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

    # Insert a new entry into the database.
    # Param DeviceID The ID given to the DAQ module.
    # Param numSensors The number of sensors registered to the device.
    # Param data the data to be inserted into the database. Inserted data
    #      should be of the form ["NULL" (or timestamp), Sensor1 data,
    #      sensor2 data, ..., SensorN data]
    # Return True if the data was successfully inserted, false otherwise.
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

    # Find the sample rate for a given DAQ module.
    # Param deviceID The ID given to the DAQ module.
    # Return The sample rate of the device as defined in
    #      `Time Interval (Min)` column of the Devices table.
    def getSampleRate(self, deviceID):
        try:
            self.c.execute("SELECT `Time Interval (Min)` FROM `" + self.deviceTable + "` WHERE "
                           + "`Device ID` = " + `deviceID`)
            return self.c.fetchall()[0][0]
        except:
            pass

    # Find the number of Devices registered in the system. This 
    #      function ignores the Eagle Eye as it behaves differently
    #      from all other DAQ modules.
    # Return The number of devices registered in the system.
    def getDeviceCount(self):
        try:
             self.c.execute("SELECT COUNT(*) FROM `" + self.deviceTable + "` WHERE `Device ID` > 0")
             return int(self.c.fetchall()[0][0])
        except:
            pass
