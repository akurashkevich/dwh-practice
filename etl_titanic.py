import pandas as pd

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