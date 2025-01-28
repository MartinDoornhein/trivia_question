# Module exquizdb.py
import psycopg2 as pg2
import psycopg2.extras as extras
import pandas as pd


class Database:
    def __init__(self, db, username, password, port):
        
        
        
        self.db = db
        self.username = username
        self.password = password
        self.port = port
        self.cur = None
        self.conn = None

    def connect(self) -> None:
        """
        Make connections to the PostgreSQL database.

        This method sets up a connection to the PostgreSQL database
        using the provided parameters.

        Returns:
            None
        """
        # Create a connection to the PostgreSQL database
        self.conn = pg2.connect(
            database=self.db, user=self.username, password=self.password, port=self.port
        )

        # Create a cursor to execute queries
        self.cur = self.conn.cursor()

    def execute_query(self, query) -> None:
        """execute custom query"""

        self.cur.execute(query)
        self.conn.commit()
        print(f"executed query: \n\t{query}")

    def create_schema(self, table_name: str, schema: str) -> None:
        """create schema in pg"""

        query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({schema}); """
        self.execute_query(query)
        print("created/updated schema succesful")

    def put_df(self, df: pd.DataFrame, table: str) -> None:
        """put pandas df to sql"""

        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ",".join(list(df.columns))

        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = self.conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            self.conn.commit()
        except (Exception, pg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        print("pd dataframe is inserted")

    def delete_date_frame(self, df: pd.DataFrame, table_name: str, ts_col: str) -> None:
        """delete rows within specified daterange/timestamp range"""
        
        try:
            max = df["timestamp"].max()
            min = df["timestamp"].min()
            query = f""" DELETE FROM {table_name} where {ts_col} BETWEEN '{min}' AND '{max}' """
            self.cur.execute(query)
            self.conn.commit()
            print(f"deleted daterange: {min} - {max}")
        except Exception as error:
            print(error)

    def close(self) -> None:
        """ "close db connection"""

        self.cur.close()
        self.conn.close()
