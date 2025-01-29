import requests
import pandas as pd

class TriviaQuestion:
    def __init__(self, question:str, category:str, difficulty:str, type:str):
        self.question = question
        self.category = category
        self.difficulty = difficulty
        self.type = type
        self.base_url  = f"https://opentdb.com/api.php?amount={self.question}"
        self.url = ""

    def get_categories(self):
        """
        Retrieve trivia categories from the Open Trivia DB API.

        Returns:
            list: A list of dictionaries, where each dictionary contains the
                'id' and 'name' of a trivia category.

        Notes:
            The API endpoint for retrieving categories is
                https://opentdb.com/api_category.php
            The API returns a JSON object with a single key, 'trivia_categories',
                which is a list of dictionaries, where each dictionary has 'id'
                and 'name' keys.
        """
        # Send the request to the API and get the response
        response = requests.get(self.url)

        # Convert the response to JSON
        json = response.json()

        # Extract the categories from the JSON
        categories = json['trivia_categories']

        # Return the categories
        return categories


    def create_url(self):
        """
        Create a URL for the API query based on the parameters.

        The URL is constructed by adding the relevant query parameters to
        the base URL. The query parameters are:

        - category: The category of the trivia questions.
        - difficulty: The difficulty of the trivia questions.
        - type: The type of the trivia questions.

        Returns:
            str: The URL for the API query.
        """
        self.url = self.base_url

        # Add the query parameters to the URL
        if self.category != "":
            self.url += f"&category={self.category}"
        if self.difficulty != "":
            self.url += f"&difficulty={self.difficulty}"
        if self.type != "":
            self.url += f"&type={self.type}"
        
        # Return the URL
        return self.url

    def get_questions(self):
    
        # Create the URL for the API query
        """
        Retrieve trivia questions from the Open Trivia DB API.

        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved trivia
                questions. The columns of the DataFrame are the keys in the
                JSON objects returned by the API, and the index is the
                question number.
        """
        url = self.create_url()

        # Send the request to the API and get the response
        response = requests.get(url)

        # Convert the response to JSON
        json = response.json()

        # Convert the JSON to a DataFrame
        df = pd.DataFrame(json['results'])

        # Return the DataFrame
        return df

