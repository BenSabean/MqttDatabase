import MySQLdb

# This calss acts as a wrapper for the MySQLdb class.
# TYpical use includes inserting new data into the databse.
class DataEntry:

    # Initialization routine.
    def __init__(self, myDb, myCursor):
        self.c = myCursor
        self.db = myDb

    # Creates a query to insert data into the database.
    # Param table The table to insert data into.
    # Param arg The data to insert into the table.
    # Return The query as a string.
    def createInsertQuery(self, table, arg):
        try:
            query = "INSERT INTO `" + table + "` VALUES ("
            for x in arg:
                query += x + ","
            query = query[:-1]
            query += ')'
        except Exception as e:
            pass
        return query

    # Close the connection to the database.
    def close(self):
        self.db.close()

    # Get the contents of the selected table.
    # Param table The table whose contents will be found.
    # Return the contents of the table as a double indexed list.
    def getTable(self, table):
        self.c.execute("SELECT * FROM " + table)
        return self.c.fetchall()

    # Insert a line of data into the database.
    # Param table The table to insert data into.
    # Param data The data to insert into tha database
    # Return True if the data is successfully inserted, False otherwise.
    def insertData(self, table, data):
        arg = []
        for n in data:
            arg.append(n)

        try:
            self.c.execute(self.createInsertQuery(table,arg))
            self.db.commit()
            return True
        except:
            self.db.rollback()
        return False
