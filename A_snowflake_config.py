

from dotenv import load_dotenv
import os
import snowflake.connector
from pathlib import Path

# Charger les variables d'environnement
load_dotenv()

snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_role_password = os.getenv("SNOWFLAKE_ROLE_PASSWORD")

conn = snowflake.connector.connect(
    account=snowflake_account,
    user=snowflake_user,
    password=snowflake_password
)

with conn.cursor() as cur:
    with open('SQL/create_role.sql', 'r') as create_role:
        cur.execute(create_role.read())
    with open('SQL/create_infrastructure.sql', 'r') as create_infrastructure:
        cur.execute(create_infrastructure.read(), (snowflake_role_password,))
    with open('SQL/grant_role.sql', 'r') as grant_role:
        cur.execute(grant_role.read())
        
