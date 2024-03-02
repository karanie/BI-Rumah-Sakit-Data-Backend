import pandas as pd

def filterInYear(df, column, year):
    return df.loc[df[column].dt.year == year]

def filterInMonth(df, column, month):
    return df.loc[df[column].dt.month == month]

def filterInYearMonth(df, column, year, month):
    return df.loc[(df[column].dt.year == year)
              & (df[column].dt.month == month)]
