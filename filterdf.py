import pandas as pd

def filter_in_year(df, column, year):
    return df.loc[df[column].dt.year == year]

def filter_in_month(df, column, month):
    return df.loc[df[column].dt.month == month]

def filter_in_year_month(df, column, year, month):
    return df.loc[(df[column].dt.year == year)
              & (df[column].dt.month == month)]
