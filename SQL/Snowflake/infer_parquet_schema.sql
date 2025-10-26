USE ROLE NYCTRANSFORM;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TEMP STAGE temp_parquet_stage;

CREATE OR REPLACE TEMP FILE FORMAT parquet_format TYPE = PARQUET;

PUT file:///home/utilisateur/Projets/SNWFLK__DBT/yellow_tripdata_2024-01.parquet @temp_parquet_stage AUTO_COMPRESS=FALSE;

SELECT
    COLUMN_NAME,
    TYPE,
    NULLABLE,
    EXPRESSION,
    ORDER_ID
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@temp_parquet_stage',
        FILE_FORMAT => 'parquet_format'
    )
)
ORDER BY ORDER_ID;