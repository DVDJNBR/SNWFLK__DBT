from dotenv import load_dotenv
import os
import snowflake.connector
from pathlib import Path
from loguru import logger
from io import StringIO

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

sf_dir = Path('SQL/Snowflake/')

logger.info("Exécution create_role.sql")
with open(sf_dir / 'create_role.sql', 'r') as f:
    for cur in conn.execute_stream(f):
        for result in cur:
            logger.debug(f"Requête exécutée: {cur.rowcount} lignes affectées")
logger.success("create_role.sql OK")

logger.info("Exécution create_infrastructure.sql")
with open(sf_dir / 'create_infrastructure.sql', 'r') as f:
    sql = f.read().replace('{?}', f"'{snowflake_role_password}'")
    sql_stream = StringIO(sql)
    for cur in conn.execute_stream(sql_stream):
        for result in cur:
            logger.debug(f"Requête exécutée: {cur.rowcount} lignes affectées")
logger.success("create_infrastructure.sql OK")

logger.info("Exécution grant_permissions.sql")
with open(sf_dir / 'grant_permissions.sql', 'r') as f:
    for cur in conn.execute_stream(f):
        for result in cur:
            logger.debug(f"Requête exécutée: {cur.rowcount} lignes affectées")
logger.success("grant_permissions.sql OK")
# with open('SQL/create_role.sql') as create_role_sql_file:
#     create_role_sql = ' '.join([query.strip()+';' for query in create_role_sql_file.read().split(';')][:-1])
# with open(SQL/create_infrastructure.sql) as create_infrastructure_sql_file:
#     create_infrastructure_sql = ' '.join([query.strip()+';' for query in create_infrastructure_sql_file.read().split(';')][:-1])


# with conn.cursor() as cur:
#     logger.info("Exécution create_role.sql")
#     with open('SQL/create_role.sql', 'r') as f:
#         queries = [q.strip() for q in f.read().strip().split(';') if q.strip()]
#         for query in queries:
#             cur.execute(query)
#     logger.success("create_role.sql OK")

#     logger.info("Exécution create_infrastructure.sql")
#     with open('SQL/create_infrastructure.sql', 'r') as f:
#         sql = f.read().replace('{?}', f"'{snowflake_role_password}'")
#         queries = [q.strip() for q in sql.strip().split(';') if q.strip()]
#         for query in queries:
#             cur.execute(query)
#     logger.success("create_infrastructure.sql OK")

#     logger.info("Exécution grant_permissions.sql")
#     with open('SQL/grant_permissions.sql', 'r') as f:
#         queries = [q.strip() for q in f.read().strip().split(';') if q.strip()]
#         for query in queries:
#             cur.execute(query)
#     logger.success("grant_permissions.sql OK")
        
