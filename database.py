import json
import mysql.connector
from mysql.connector import errorcode
import pandas as pd

class challengedb():
    def __init__(self):
        with open(r"configdb.JSON", "r") as file:
            config = json.load(file)


        # connect to mysql
        try:
            self.cnx = mysql.connector.connect(**config)
            self.cursor = self.cnx.cursor(buffered=True)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            exit(1)

                
    def save(self):

        self.cnx.commit()

    def get_movies(self):
        #TODO: add arguments to the stored procedure to make it more flexible
        args = ()
        self.cursor.callproc('setup_tag_chain',  args)

        result =  next(self.cursor.stored_results()).fetchall()


        df = pd.DataFrame(result)
        df.columns = ["id", "title", "finish"]
        return df