from openrouter import OpenRouter
from dotenv import load_dotenv
import pyodbc
import os
import pandas as pd
from sqlalchemy import create_engine
import urllib
import json
from datetime import date

class GenAIEngine:

    def __init__(self):
        load_dotenv()
        # Initialize OpenRouter client
        self.client = OpenRouter(api_key=os.getenv("OPENROUTER_API_KEY", ""))

        # Build DB connection string
        connection_string = (
            f"Driver={{{os.getenv('DB_DRIVER')}}};"
            f"Server={os.getenv('DB_SERVER')};"
            f"Database={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
        )
        params = urllib.parse.quote_plus(connection_string)
        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    # 1️⃣ Prompt Builder
    def build_company_profile_prompt(self, row):
        prompt = f"""
        You are an AI analyst. Analyze the following company information and produce structured insights.

        Company Name: {row['company_name']}
        Base URL: {row['base_url']}
        Page Title: {row['title']}

        Cleaned Text:
        {row['cleaned_text']}

        Raw Text (for additional context):
        {row['raw_text']}

        Please return the output in JSON format with the following fields:
        - summary
        - services
        - industries
        - target_audience
        - positioning_tone
        - ai_maturity_guess
        - confidence_score
        """
        return prompt.strip()

    # 2️⃣ LLM Caller (OpenRouter)
    def call_llm(self, prompt: str) -> str:
        response = self.client.chat.send(
            model="x-ai/grok-4-fast",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ]
        )
        return response.choices[0].message.content

    # 3️⃣ Response Parser
    def parse_llm_response(self, response_text):
        try:
            insights = json.loads(response_text)
        except json.JSONDecodeError:
            # fallback if not valid JSON
            insights = {
                "summary": response_text,
                "services": None,
                "industries": None,
                "target_audience": None,
                "positioning_tone": None,
                "ai_maturity_guess": None,
                "confidence_score": None
            }
        return insights

    # 4️⃣ Insight Saver
    def save_insights(self, company_id, insights):
        conn = pyodbc.connect(
            f"Driver={{{os.getenv('DB_DRIVER')}}};"
            f"Server={os.getenv('DB_SERVER')};"
            f"Database={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
        )
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO company_insights (
                company_id, summary, services, industries, target_audience,
                positioning_tone, ai_maturity_guess, confidence_score, enriched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            company_id,
            insights.get("summary"),
            insights.get("services"),
            insights.get("industries"),
            insights.get("target_audience"),
            insights.get("positioning_tone"),
            insights.get("ai_maturity_guess"),
            insights.get("confidence_score"),
            date.today()
        ))

        cursor.execute("""
            INSERT INTO logs (stage, status, message, created_at)
            VALUES (?, ?, ?, ?)
        """, ("Enrichment", "Success", f"Enriched company {company_id}", date.today()))

        conn.commit()
        conn.close()

    # 5️⃣ Orchestration
    def enrich_companies(self):
        query = """
            SELECT c.id, base_url, company_name, title, raw_text, cleaned_text 
            FROM companies c
            JOIN pages p ON c.id = p.company_id
        """
        df = pd.read_sql(query, self.engine)

        for _, row in df.iterrows():
            prompt = self.build_company_profile_prompt(row)
            response_text = self.call_llm(prompt)
            insights = self.parse_llm_response(response_text)
            self.save_insights(row['id'], insights)

if __name__ == '__main__':
    run = GenAIEngine()
    run.enrich_companies()
