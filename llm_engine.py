from openai import OpenAI
from dotenv import load_dotenv
import pyodbc
import os
from scraper_db import Scraper
import pandas as pd
from sqlalchemy import create_engine
import urllib

class GenAIEngine:

    def read_data(self):
        load_dotenv()

        connection_string = (
            f"Driver={{{os.getenv('DB_DRIVER')}}};"
            f"Server={os.getenv('DB_SERVER')};"
            f"Database={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
        )

        params = urllib.parse.quote_plus(connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        qwery = """
            SELECT c.id, base_url, company_name, title, raw_text, cleaned_text 
            FROM companies c
            JOIN pages p ON c.id = p.company_id
            """

        self.df = pd.read_sql(qwery, engine)

        print(self.df)

if __name__ == '__main__':
    run = GenAIEngine()
    run.read_data()
