# %%
import pandas as pd

file_path = 'titanic_cleaned.csv'

try:
    df = pd.read_csv(file_path)
    print(f"Successfully loaded data from {file_path}")
    print(f"Shape of the DataFrame: {df.shape}")
    
except FileNotFoundError:
    print(f"Error: The file at {file_path} was not found.")
    exit()
# %%
print(df.describe())
# %%
print(df['sex'].value_counts())
# %%
print(df.groupby('pclass')['survived'].mean())
print(df.groupby('embarked')['survived'].mean())
# %%
df.groupby('pclass')['fare'].mean()
# %%
