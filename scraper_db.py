import requests 
import pandas as pd
import os
from bs4 import BeautifulSoup, SoupStrainer
from dotenv import load_dotenv
import pyodbc
from datetime import date


class Scraper:

    def __init__(self):
        self.path = r"C:\Users\karth\Downloads\Python_Project\GenAICompetitorIntelligenceEngine"
        self.file_name = 'company.csv'
        self.file = os.path.join(self.path, self.file_name)


    def fetch_url(self, df: pd.DataFrame) -> pd.DataFrame:
        self.text = []
        self.url = []

        if not df.empty:

            for index, row in df.iterrows():
                self.url.append(row['URL'])

            max_retry = 3

            for link in self.url:
                retry_value = 0
                success = False

                while retry_value < max_retry and not success:
                    try:
                        response = requests.get(link, timeout=5)
                        if response.status_code == 200:
                            print(f"fetching text for URL: {link}")
                            only_a_tags = SoupStrainer( [
                                                            "title",          
                                                            "h1", "h2", "h3", 
                                                            "p",              
                                                            "meta"
                                                        ])
                            soup = BeautifulSoup(response.text, 'html.parser', parse_only=only_a_tags)
                            meta = soup.find("meta", attrs={"name": "description"})
                            self.text.append({
                                "URL": link,
                                "title": soup.title.get_text(strip=True) if soup.title else None,
                                "headings": [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2', 'h3'])],
                                "paragraphs": [p.get_text(strip=True) for p in soup.find_all('p')],
                                "description": meta['content'].strip() if meta and meta.has_attr("content") else None
                                                    })
                            success = True
                        else:
                            retry_value += 1
                            print(f"Error fetching text: {response.status_code}")
                            print(f"Retrying for {retry_value} time. MAX Retry: {max_retry}")
                            

                    except requests.RequestException as e:
                        retry_value += 1
                        print("Unable to fetch the or text or estavlish connection")
                        print(f"Retrying for {retry_value} time. MAX Retry: {max_retry}")
        else:
            print("Data Frame caanot be empty")


    def clean_text(self):

        self.cleaner_text = []

        junk_words = ["home", "privacy policy", "contact Us","about us"]

        for txt in self.text:
            headings = list(set(txt["headings"]))
            paragraphs = list(set(txt["paragraphs"]))
            headings = [h.strip() for h in headings if h.lower() not in junk_words]
            paragraphs = [p.strip() for p in paragraphs if p.lower() not in junk_words]

            self.cleaner_text.append({
                        "URL": txt["URL"],
                        "title": txt["title"],
                        "headings": headings,
                        "paragraphs": paragraphs,
                        "description": txt["description"].strip() if txt["description"] else None
            })

        
        df1 = pd.DataFrame(self.cleaner_text)        
        self.df2 = pd.read_csv(self.file)
        dup_col = [c for c in df1.columns if c in self.df2.columns and c != 'URL']

        self.df2 = pd.merge(df1, self.df2.drop(columns=dup_col), on='URL')

        print(self.df2)



    def create_table_schema(self):

        load_dotenv()

        conn = pyodbc.connect(
            f"Driver={{{os.getenv('DB_DRIVER')}}};"
            f"Server={os.getenv('DB_SERVER')};"
            f"Database={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
        )
        cursor = conn.cursor()

        cursor.execute("IF OBJECT_ID('companies', 'U') IS NOT NULL DROP TABLE companies")
        cursor.execute("IF OBJECT_ID('pages', 'U') IS NOT NULL DROP TABLE pages")
        cursor.execute("IF OBJECT_ID('company_insights', 'U') IS NOT NULL DROP TABLE company_insights")
        cursor.execute("IF OBJECT_ID('logs', 'U') IS NOT NULL DROP TABLE logs")

        cursor.execute("""
                        CREATE TABLE companies (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        base_url NVARCHAR(500) NOT NULL,
                        company_name NVARCHAR(255),
                        scraped_at DATETIME DEFAULT GETDATE());""")
        
        cursor.execute("""
                        CREATE TABLE pages (
                            id INT IDENTITY(1,1) PRIMARY KEY,
                            company_id INT NOT NULL,
                            url NVARCHAR(500) NOT NULL,
                            title NVARCHAR(255),
                            raw_text NVARCHAR(MAX),
                            cleaned_text NVARCHAR(MAX),
                            scraped_at DATETIME DEFAULT GETDATE(),
                            FOREIGN KEY (company_id) REFERENCES companies(id)
                        );

                       """)


        cursor.execute("""
                            CREATE TABLE company_insights (
                                id INT IDENTITY(1,1) PRIMARY KEY,
                                company_id INT NOT NULL,
                                summary NVARCHAR(MAX),
                                services NVARCHAR(MAX),
                                industries NVARCHAR(MAX),
                                target_audience NVARCHAR(MAX),
                                positioning_tone NVARCHAR(MAX),
                                ai_maturity_guess NVARCHAR(255),
                                confidence_score FLOAT,
                                enriched_at DATETIME DEFAULT GETDATE(),
                                FOREIGN KEY (company_id) REFERENCES companies(id));""")


        cursor.execute("""
                                CREATE TABLE logs (
                                    id INT IDENTITY(1,1) PRIMARY KEY,
                                    stage NVARCHAR(255),
                                    status NVARCHAR(50),
                                    message NVARCHAR(MAX),
                                    created_at DATETIME DEFAULT GETDATE());""")


        conn.commit()

    
    def insert_data(self):
        load_dotenv()

        conn = pyodbc.connect(
            f"Driver={{{os.getenv('DB_DRIVER')}}};"
            f"Server={os.getenv('DB_SERVER')};"
            f"Database={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASS')};"
        )
        cursor = conn.cursor()

        for _, row in self.df2.iterrows():
            cursor.execute("""
                INSERT INTO companies (base_url, company_name, scraped_at)
                VALUES (?, ?, ?)""",
                (row['URL'], row['Company'], date.today()))

            cursor.execute("SELECT @@IDENTITY")
            company_id = cursor.fetchone()[0]

            for page in self.cleaner_text:
                if page['URL'] == row['URL']: 
                    cursor.execute("""
                        INSERT INTO pages (company_id, url, title, raw_text, cleaned_text, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (company_id, page['URL'], page['title'],
                        " ".join(page['paragraphs']),   
                        " ".join(page['headings']),     
                        date.today()))

                    cursor.execute("""
                        INSERT INTO logs (stage, status, message, created_at)
                        VALUES (?, ?, ?, ?)""",
                        ("Insert", "Success", f"Inserted page {page['URL']}", date.today()))

            # 4️⃣ Company Insights (placeholder, add after enrichment)
            # Example insert once you have LLM results:
            # cursor.execute("""
            #     INSERT INTO company_insights (company_id, summary, services, industries, target_audience, positioning_tone, ai_maturity_guess, confidence_score, enriched_at)
            #     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            #     (company_id, summary, services, industries, audience, tone, maturity, confidence, date.today()))

        conn.commit()
        conn.close()




    def main(self):
        df = pd.read_csv(self.file)
        self.fetch_url(df)
        self.clean_text()
        self.create_table_schema()
        self.insert_data()



if __name__ == '__main__':
    run = Scraper()
    run.main()