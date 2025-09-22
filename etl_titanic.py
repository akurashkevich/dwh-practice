import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import clickhouse_connect
import logging
import pandas as pd
import re

def camel_to_snake(name):
    """
    Конвертирует строку из CamelCase в snake_case.
    """
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

logging.basicConfig(level=logging.INFO, filename='etl.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()
logging.info("Environment variables loaded.")

PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_HOST = os.getenv('POSTGRES_HOST', 'localhost')
PG_PORT = os.getenv('POSTGRES_PORT')
PG_DB = os.getenv('POSTGRES_DB')

CH_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT = os.getenv('CLICKHOUSE_PORT', '8123')
CH_USER = os.getenv('CLICKHOUSE_USER')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CH_DB = os.getenv('CLICKHOUSE_DB', 'default')

# --- EXTRACT ---
logging.info("--- Starting EXTRACT step ---")
try:
    pg_conn_str = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}'
    pg_engine = create_engine(pg_conn_str)
    
    df = pd.read_sql_query('SELECT * FROM titanic_raw', pg_engine)
    
    logging.info(f"Successfully extracted {len(df)} rows from PostgreSQL.")
except Exception as e:
    logging.error(f"Error during EXTRACT step: {e}")
    exit()

# --- Initial Inspection ---
print("\nFirst 5 rows of the DataFrame:")
print(df.head())

print("\nDataFrame Info:")
df.info()

# --- TRANSFORM ---
print("\nStarting Transform step...")

# Шаг 1: Очистка названий колонок
# Создаем копию DataFrame, чтобы не изменять оригинал напрямую.
df_transformed = df.copy()

df_transformed.columns = [camel_to_snake(col) for col in df_transformed.columns]

print("Cleaned column names:")
print(df_transformed.columns)

# Шаг 2: Обработка пропусков в 'age'
# Вычисляем медианный возраст
median_age = df_transformed['age'].median()
print(f"\nMedian age: {median_age}")

# Заполняем все пропуски в колонке 'age' вычисленной медианой
df_transformed['age'] = df_transformed['age'].fillna(median_age)

# Шаг 3: Удаление колонок с большим количеством пропусков
df_transformed.drop(columns=['cabin'], inplace=True)

# Шаг 4: Заполнение пропусков в 'embarked'
# Находим самое частое значение
most_frequent_port = df_transformed['embarked'].mode()[0]
print(f"Most frequent port in 'embarked' is: {most_frequent_port}")

# Заполняем пропуски этим значением
df_transformed['embarked'] = df_transformed['embarked'].fillna(most_frequent_port)

# Шаг 5: Создание новой колонки 'family_size'
df_transformed['family_size'] = df_transformed['sib_sp'] + df_transformed['parch'] + 1
print("\nCreated 'family_size' feature.")

# Шаг 6: Удаление ненужных и избыточных колонок
columns_to_drop = ['passenger_id', 'name', 'ticket', 'sib_sp', 'parch']
df_transformed.drop(columns=columns_to_drop, inplace=True)
print(f"\nDropped unnecessary columns: {columns_to_drop}")

print("\nTransformation step completed.")

# --- LOAD ---
logging.info("--- Starting LOAD step ---")
try:
    import clickhouse_connect

    table_name = 'cleaned_titanic'
    
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=int(CH_PORT),
        user=CH_USER, 
        password=CH_PASSWORD, 
        database=CH_DB
    )
    logging.info("Successfully connected to ClickHouse.")

    client.command(f'DROP TABLE IF EXISTS {table_name}')
    logging.info(f"Table {table_name} dropped if existed.")

    create_table_ddl = f"""
    CREATE TABLE {table_name} (
        survived Int64,
        pclass Int64,
        sex String,
        age Float64,
        fare Float64,
        embarked String,
        family_size Int64
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    client.command(create_table_ddl)
    logging.info(f"Table {table_name} created with MergeTree engine.")

    client.insert_df(table_name, df_transformed)
    logging.info(f"Successfully loaded {len(df_transformed)} rows into ClickHouse.")
    logging.info("ETL process completed successfully.")

except Exception as e:
    logging.error(f"Error during LOAD step: {e}")

print("\nFinal DataFrame head:")
print(df_transformed.head())

print("\nFinal DataFrame Info:")
df_transformed.info()