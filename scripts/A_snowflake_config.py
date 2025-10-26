from dotenv import load_dotenv
import os
import snowflake.connector
from pathlib import Path
from loguru import logger
from io import StringIO

load_dotenv()
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_role_password = os.getenv("SNOWFLAKE_ROLE_PASSWORD")

# Connexion à Snowflake
conn = snowflake.connector.connect(
    account=snowflake_account,
    user=snowflake_user,
    password=snowflake_password
)

sf_dir = Path('SQL/Snowflake/')

### CLEANUP - Nettoyer l'infrastructure existante pour un démarrage propre
logger.info("🧹 Nettoyage de l'infrastructure existante...")
cursor = conn.cursor()
cursor.execute("USE ROLE ACCOUNTADMIN")

cleanup_commands = [
    "DROP DATABASE IF EXISTS NYC_TAXI_DB CASCADE",
    "DROP WAREHOUSE IF EXISTS NYC_TAXI_WH", 
    "DROP USER IF EXISTS NYCDBT",
    "DROP ROLE IF EXISTS NYCTRANSFORM"
]

for cmd in cleanup_commands:
    try:
        cursor.execute(cmd)
        logger.debug(f"Cleanup: {cmd}")
    except Exception as e:
        logger.warning(f"Cleanup warning: {cmd} - {e}")

cursor.close()
logger.success("🧹 Cleanup terminé - Environnement propre")


### CREATING ROLE
with open(sf_dir / 'create_role.sql', 'r') as f:
    # queries are ; separated so the whole file needs to be executed as a stream as StringIO
    for cur in conn.execute_stream(StringIO(f.read())):
        for result in cur:
            logger.debug(f"Query executed: {cur.rowcount} rows affected")
logger.success("create_role.sql OK")

### CREATING WAREHOUSE AND DATABASE
with open(sf_dir / 'create_infrastructure.sql', 'r') as f:
    sql = f.read().replace('{?}', f"'{snowflake_role_password}'")
    sql_stream = StringIO(sql)
    for cur in conn.execute_stream(sql_stream):
        for result in cur:
            logger.debug(f"Query executed: {cur.rowcount} rows affected")
logger.success("create_infrastructure.sql OK")

### GRANTING PERMISSIONS
with open(sf_dir / 'grant_permissions.sql', 'r') as f:
    for cur in conn.execute_stream(StringIO(f.read())):
        for result in cur:
            logger.debug(f"Query executed: {cur.rowcount} rows affected")
logger.success("grant_permissions.sql OK")

### CREATING TAXI TRIPS TABLE
with open(sf_dir / 'create_taxi_trips_table.sql', 'r') as f:
    for cur in conn.execute_stream(StringIO(f.read())):
        for result in cur:
            logger.debug(f"Query executed: {cur.rowcount} rows affected")
logger.success("create_taxi_trips_table.sql OK")