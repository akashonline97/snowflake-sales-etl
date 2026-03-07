import snowflake.connector
import logging, os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting Snowflake pipeline")
conn = snowflake.connector.connect (
    user=os.getenv("user"),
    password=os.getenv("password"),
    account=os.getenv("account"),
    warehouse=os.getenv("warehouse"),
    database=os.getenv("database"),
    schema=os.getenv("schema")
)

try:
    cur = conn.cursor()
    with open("/Users/akash/Documents/FASTAPI/snowflake-first-project/sql/transformation.sql", "r") as f:
        sql_commands = f.read()
    for i in sql_commands.split(";"):
        if i.strip():
            cur.execute(i)
    logging.info("Pipeline executed successfully")
finally:
    cur.close()
    conn.close()