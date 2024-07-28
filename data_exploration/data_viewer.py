import csv
import pandas as pd

file = 'data/conversations.csv'

df = pd.read_csv(file)
print(df.head())
print(df.columns)

# show data type
print("\nData types:")
print(df.dtypes)

# show which columns have missing values
print("\nColumns with missing values:")
print(df.isnull().sum())

# number of unique values in each column
print("\nNumber of unique values in each column:")
print(df.nunique())

# max and min values in each column
print("\nMax and min values in each column:")
print(df.max())
print(df.min())
