import prophet
import polars as pl

def predict(df, periods, ds_col="ds", y_col="y"):
    m = prophet.Prophet()
    df = df.select(pl.col(ds_col).alias("ds"), pl.col(y_col).alias("y"))
    df = df.to_pandas()
    m.fit(df)
    future = m.make_future_dataframe(periods=periods)
    forecast = m.predict(future)
    return forecast
