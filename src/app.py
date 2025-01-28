from trivia_question import TriviaQuestion
from database_connector import Database
import pandas as pd


n_questions = 20
category = ""
difficulty = "medium"
type = "boolean"

def main():
    """
    Main function to execute the trivia question retrieval and database storage.

    This function first instantiates a TriviaQuestion object with the given parameters.
    It then calls the get_questions method on the object to retrieve the questions
    and stores the returned DataFrame in the variable df.

    The function then connects to a PostgreSQL database using a Database object
    and executes the query created by the infer_df_schema method to create the table
    in the database. The function then calls the put_df method to store the DataFrame
    in the table.

    Finally, the function calls the close method on the Database object to close
    the connection to the database.
    """
    # Instantiate a TriviaQuestion object
    questions = TriviaQuestion(n_questions, category, difficulty, type)

    # Retrieve the trivia questions
    df = questions.get_questions()

    # Connect to a PostgreSQL database
    db = Database(db="postgres", username="postgres", password="12450", port=5432)
    db.connect()

    # Create the table in the database
    query = db.infer_df_schema("trivia_questions", df)
    db.execute_query(query)

    # Store the DataFrame in the table
    db.put_df(df, "trivia_questions")

    # Close the connection to the database
    db.close()



if __name__ == "__main__":
    main()
