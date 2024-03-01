import pandas pd

def filterInYear(df, column, year):
    return df[df[column].dt.year == year]

def filterInMonth(df, column, month):
    return df[df[column].dt.month == month]

def filterInYearMonth(df, column, year, month):
    return df[(df[column].dt.year == year)
              & (df[column].dt.month == month)]
