import psycopg2
import os


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
        with open('migrations_ts/migrations_ts.sql', 'r') as file:
            sql_statements = file.read().split(';')
    
        for statement in sql_statements:
            if statement.strip():
                self.cursor.execute(statement)

    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    def execute(self, query, params=None):
        if params:
            return self.cursor.execute(query, params)
        else:
            return self.cursor.execute(query)
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

        
     
         