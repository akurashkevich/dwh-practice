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

def extract_from_postgres(table_name):
    """
    Извлекает все данные из указанной таблицы в PostgreSQL.
    :param table_name: Имя таблицы для извлечения.
    :return: DataFrame с данными или None в случае ошибки.
    """
    logging.info(f"--- Starting EXTRACT step for table: {table_name} ---")
    try:
        pg_conn_str = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}'
        pg_engine = create_engine(pg_conn_str)
        
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql_query(query, pg_engine)
        
        logging.info(f"Successfully extracted {len(df)} rows from PostgreSQL.")
        return df
    
    except Exception as e:
        logging.error(f"Error during EXTRACT step: {e}")
        return None

def transform_data(df):
    """
    Применяет все шаги трансформации к DataFrame "Титаника".
    :param df: "грязный" DataFrame после извлечения.
    :return: "чистый", трансформированный DataFrame или None в случае ошибки.
    """
    logging.info("--- Starting TRANSFORM step ---")

    try:
        logging.info("Starting data transformation...")
        df_transformed = df.copy()

        # 1. Очистка названий колонок
        df_transformed.columns = [camel_to_snake(col) for col in df_transformed.columns]

        # 2. Обработка пропусков
        median_age = df_transformed['age'].median()
        df_transformed['age'] = df_transformed['age'].fillna(median_age)
        
        most_frequent_port = df_transformed['embarked'].mode()[0]
        df_transformed['embarked'] = df_transformed['embarked'].fillna(most_frequent_port)

        # 3. Feature Engineering
        df_transformed['family_size'] = df_transformed['sib_sp'] + df_transformed['parch'] + 1

        # 4. Удаление ненужных колонок
        columns_to_drop = ['passenger_id', 'name', 'ticket', 'cabin', 'sib_sp', 'parch']
        df_transformed.drop(columns=columns_to_drop, inplace=True)
        
        logging.info("Data transformation completed successfully.")
        return df_transformed
    
    except Exception as e:
        logging.error(f"Error during TRANSFORM step: {e}")
        return None

def load_to_clickhouse(df, table_name):
    """
    Загружает DataFrame в указанную таблицу в ClickHouse.
    :param df: DataFrame для загрузки.
    :param table_name: Имя целевой таблицы.
    """
    logging.info(f"--- Starting LOAD step for table: {table_name} ---")

    try:        
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

        client.insert_df(table_name, df)
        logging.info(f"Successfully loaded {len(df)} rows into ClickHouse.")
        logging.info("ETL process completed successfully.")
        return True
    
    except Exception as e:
        logging.error(f"Error during LOAD step: {e}")
        return False

if __name__ == "__main__":
    df_raw = extract_from_postgres(table_name='titanic_raw')
    
    if df_raw is not None:
        df_transformed = transform_data(df_raw)
        
        if df_transformed is not None:
            success = load_to_clickhouse(df=df_transformed, table_name='titanic_cleaned')
            
            if success:
                logging.info("ETL process for Titanic completed successfully.")
            else:
                logging.error("ETL process for Titanic failed at LOAD step.")
        else:
            logging.error("ETL process for Titanic failed at TRANSFORM step.")
    else:
        logging.error("ETL process for Titanic failed at EXTRACT step.")