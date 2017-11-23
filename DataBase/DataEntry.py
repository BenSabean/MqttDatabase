import MySQLdb

class DataEntry:  

    def __init__(self, myDb, myCursor):
        self.c = myCursor
        self.db = myDb
        
    def createInsertQuery(self, table, arg):
        try:
            query = "INSERT INTO `" + table + "` VALUES ("
            for x in arg:
                query += x + ","
            query = query[:-1]
            query += ')'
        except:
            pass

        return query
    
    def close(self):
        self.db.close() 

    def getTable(self, table):
        self.c.execute("SELECT * FROM " + table)
        return self.c.fetchall()
    
    def insertData(self, table, data):
        arg = []
        for n in data:
            arg.append(n)
            
        try:
            print("Creating query")
            self.c.execute(self.createInsertQuery(table,arg))
            self.db.commit()
            return True
        except:
            self.db.rollback()
        return False
        
