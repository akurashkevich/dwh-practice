import pandas as pd
import re

# --- EXTRACT ---
print("Starting Extract step...")

file_path = 'titanic_raw.csv'

try:
    df = pd.read_csv(file_path)
    print(f"Successfully loaded data from {file_path}")
    print(f"Shape of the DataFrame: {df.shape}")
    
except FileNotFoundError:
    print(f"Error: The file at {file_path} was not found.")
    exit()

# --- Initial Inspection ---
print("\nFirst 5 rows of the DataFrame:")
print(df.head())

print("\nDataFrame Info:")
df.info()

def camel_to_snake(name):
    """
    Конвертирует строку из CamelCase в snake_case.
    """
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

# --- TRANSFORM ---
print("\nStarting Transform step...")

# Шаг 1: Очистка названий колонок
# Создаем копию DataFrame, чтобы не изменять оригинал напрямую.
df_transformed = df.copy()

df_transformed.columns = [camel_to_snake(col) for col in df_transformed.columns]

print("Cleaned column names:")
print(df_transformed.columns)