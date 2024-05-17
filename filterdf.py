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
