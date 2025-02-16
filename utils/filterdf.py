import functools
import pandas as pd

def filter_in_year(df, column, year):
    return df.loc[df[column].dt.year == year]

def filter_in_month(df, column, month):
    return df.loc[df[column].dt.month == month]

def filter_in_year_month(df, column, year, month):
    return df.loc[(df[column].dt.year == year)
              & (df[column].dt.month == month)]

def filter_last(df, column, from_last_data=True, **kwds):
    # Convert the column to datetime if it's not already
    df[column] = pd.to_datetime(df[column])

    # Calculate the date N months ago
    if from_last_data:
        # Berdasarkan data terakhir
        current_date = df.iloc[-1][column]
    else:
        # Berdasarkan current timestamp
        current_date = pd.Timestamp.now().date()

    n_months_ago = current_date - pd.DateOffset(**kwds)

    # Filter the DataFrame for the last N months
    filtered_df = df[df[column] >= n_months_ago]

    return filtered_df

def filter_range(df, column, start_date, end_date):
    df[column] = pd.to_datetime(df[column])
    filtered_df = df[(df[column] >= start_date) & (df[column] <= end_date)]
    return filtered_df

def filtertime(df, timeColumn, month=None, year=None, relative_time=None, start_date=None, end_date=None):
    if year is not None and month is not None:
        return filter_in_year_month(df, timeColumn, year, month)
    if year is not None:
        return filter_in_year(df, timeColumn, year)
    if relative_time is not None:
        if relative_time == "day":
            return filter_last(df, timeColumn, from_last_data=True, days=1)
        elif relative_time == "week":
            return filter_last(df, timeColumn, from_last_data=True, weeks=1)
        elif relative_time == "month":
            return filter_last(df, timeColumn, from_last_data=True, months=1)
        elif relative_time == "3month":
            return filter_last(df, timeColumn, from_last_data=True, months=3)
        elif relative_time == "6month":
            return filter_last(df, timeColumn, from_last_data=True, months=6)
        elif relative_time == "year":
            return filter_last(df, timeColumn, from_last_data=True, years=1)
    if start_date is not None and end_date is not None:
        return filter_range(df, timeColumn, start_date=start_date, end_date=end_date)
    return df

# filters schema (as far as think):
# string | number | string[] | number[]
# | { equals?: string | number, less?: string | number, more?: string | number, isin?: (string | number)[] }
def filtercols(df, filters):
    if not filters:
        return df

    def get_mask(df, column, operation):
        if isinstance(operation, dict):
            if "equal" in operation:
                print(df[column] == operation["equals"])
                return df[column] == operation["equals"]
            if "less" in operation:
                return df[column] < operation["less"]
            if "lessEqual" in operation:
                return df[column] <= operation["lessEqual"]
            if "more" in operation:
                return df[column] > operation["more"]
            if "moreEqual" in operation:
                return df[column] >= operation["moreEqual"]
            if "isin" in operation:
                return df[column].isin(operation["isin"])
        else:
            return df[column] == operation

    mask = functools.reduce(
        lambda a, b: a & b,
        [ get_mask(df, i, filters[i]) for i in filters ]
    )
    return df.loc[mask]

def resample_opt(tahun,bulan):
    if tahun is None and bulan is None:
        resample_option = "YE"
    elif tahun is not None and bulan is None:
        resample_option = "M"
    else:
        resample_option = "D"

    return resample_option

def default_filter(temp_df,kabupaten, tahun, bulan):
    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)

    return temp_df
