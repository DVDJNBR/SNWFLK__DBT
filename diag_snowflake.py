import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

def diagnostic():
    print(f"--- Diagnostic Snowflake ---")
    print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"User: {os.getenv('SNOWFLAKE_USER')}")
    
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse="NYC_TAXI_WH",
            database="NYC_TAXI_DB"
        )
        cursor = conn.cursor()
        
        print("\n1. Rôles actuels de l'utilisateur:")
        cursor.execute("SHOW ROLES")
        for row in cursor:
            print(f" - {row[1]}")
            
        print("\n2. Vérification du schéma RAW dans NYC_TAXI_DB:")
        try:
            cursor.execute("SHOW SCHEMAS IN DATABASE NYC_TAXI_DB")
            found_raw = False
            for row in cursor:
                print(f" - {row[1]}")
                if row[1].upper() == 'RAW':
                    found_raw = True
            
            if found_raw:
                print("\n3. Tables dans NYC_TAXI_DB.RAW:")
                cursor.execute("SHOW TABLES IN SCHEMA NYC_TAXI_DB.RAW")
                for row in cursor:
                    print(f" - {row[1]}")
            else:
                print("\n3. ERREUR : Le schéma RAW n'existe pas dans NYC_TAXI_DB.")
                
        except Exception as e:
            print(f"Erreur lors de l'accès à la DB: {e}")
            
        conn.close()
    except Exception as e:
        print(f"Erreur de connexion globale: {e}")

if __name__ == "__main__":
    diagnostic()
