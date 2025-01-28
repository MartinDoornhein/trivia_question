# Module exquizdb.py
import psycopg2 as pg2
import psycopg2.extras as extras
import pandas as pd


class Database:
    def __init__(self, db, username, password, port):
                        
        """
        Initialize a Database object.

        This method sets up the parameters for making connections to the PostgreSQL
        database.

        Parameters:
            db (str): The name of the database to connect to.
            username (str): The username to use for making connections.
            password (str): The password to use for making connections.
            port (int): The port number to use for making connections.

        Returns:
            None
       """
        
        self.db = db
        self.username = username
        self.password = password
        self.port = port
        self.cur = None
        self.conn = None

    def connect(self) -> None:
        """
        Establish a connection to the PostgreSQL database.

        This method initializes a connection to the PostgreSQL database
        using the provided credentials and parameters. It also sets up
        a cursor for executing queries.

        Returns:
            None
        """
        # Connect to the PostgreSQL database using the given credentials
        self.conn = pg2.connect(
            database=self.db, user=self.username, password=self.password, port=self.port
        )

        # Initialize a cursor for executing SQL queries
        self.cur = self.conn.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute a custom query against the PostgreSQL database.

        This method executes a custom query against the PostgreSQL database
        using the provided query string.

        Parameters:
            query (str): The query string to be executed against the database.

        Returns:
            None
        """
        try:
            # Execute the query
            self.cur.execute(query)

            # Commit the changes to the database
            self.conn.commit()

            # Print a message indicating that the query was executed successfully
            print(f"executed query: \n\t{query}")
        except (Exception, pg2.Error) as error:
            # Print any errors that occur during the execution of the query
            print(error)

    def create_schema(self, table_name: str, schema: str) -> None:
        """
        Create a schema in a PostgreSQL database.

        This method creates a table in the PostgreSQL database using the
        provided schema. The table is created with the specified column names
        and data types.

        Parameters:
            table_name (str): The name of the table to be created.
            schema (str): A comma-separated string of column names and data types.

        Returns:
            None
        """
        # Create the query to create the table in the PostgreSQL database
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({schema}); """

        # Execute the query to create the table
        self.execute_query(query)

        # Print a message indicating that the schema was created successfully
        print("created/updated schema succesful")

    def put_df(self, df: pd.DataFrame, table: str) -> None:
        """
        Insert a pandas DataFrame into a PostgreSQL table.

        Parameters:
            df (pd.DataFrame): The DataFrame to be inserted into the table.
            table (str): The name of the table in the PostgreSQL database.

        Returns:
            None
        """
        # Convert the DataFrame to a list of tuples
        tuples = [tuple(x) for x in df.to_numpy()]

        # Create a comma-separated string of column names
        cols = ",".join(list(df.columns))

        # SQL query for inserting data
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        
        # Create a cursor to execute the query
        cursor = self.conn.cursor()
        try:
            # Execute the query with the data
            extras.execute_values(cursor, query, tuples)
            # Commit the transaction
            self.conn.commit()
        except (Exception, pg2.DatabaseError) as error:
            # Print error and rollback if there's an issue
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        
        # Print success message
        print("pd dataframe is inserted")

    def delete_date_frame(self, df: pd.DataFrame, table_name: str, ts_col: str) -> None:
        """
        Delete the date range from the database.

        Given a pandas DataFrame `df`, this method deletes the rows from the
        table `table_name` in the PostgreSQL database where the value in column
        `ts_col` is between the minimum and maximum values in the 'timestamp'
        column of the DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame from which to extract the date range.
            table_name (str): The name of the table in the PostgreSQL database
                from which to delete the date range.
            ts_col (str): The name of the column in the table to use for
                determining the date range.

        Returns:
            None
        """
        try:
            # Get the maximum and minimum timestamp values from the DataFrame
            max = df["timestamp"].max()
            min = df["timestamp"].min()

            # Construct the DELETE query
            query = f""" DELETE FROM {table_name} where {ts_col} BETWEEN '{min}' AND '{max}' """
            self.cur.execute(query)

            # Commit the changes to the database
            self.conn.commit()

            # Print a message indicating that the deletion was successful
            print(f"deleted daterange: {min} - {max}")
        except Exception as error:
            # Print any errors that occur during the deletion
            print(error)

    def close(self) -> None:
        """ "close db connection"""

        self.cur.close()
        self.conn.close()
