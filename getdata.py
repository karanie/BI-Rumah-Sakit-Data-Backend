import os
import pickle
import numpy as np
import pandas as pd
from darts.timeseries import TimeSeries
from darts.models import ExponentialSmoothing

def get_aggr_func(aggr_str):
    map = {
        "sum": np.sum,
        "mean": np.mean,
    }
    return map[aggr_str]

def get_time_series_df(df, timeCol=None, resample="D", categoricalCols=[], numericalCols=[], pivot=False, agg="sum"):
    if not categoricalCols and not numericalCols:
        raise Exception("Either categoricalCols or numericalCols is required")

    temp_df = df[[*categoricalCols, *numericalCols, timeCol]].set_index(timeCol)

    if categoricalCols and numericalCols and not pivot:
        temp_df_cat = pd.crosstab(temp_df.index, [ temp_df[i] for i in categoricalCols ])
        temp_df_num = temp_df[[*numericalCols]]
        temp_df = temp_df_cat.merge(temp_df_num, left_index=True, right_index=True)
    elif categoricalCols and numericalCols and pivot:
        temp_df = temp_df.pivot_table(index=[timeCol], columns=categoricalCols, values=(numericalCols if len(numericalCols) > 1 else numericalCols[0]), fill_value=0)
    elif categoricalCols and not numericalCols:
        temp_df = pd.crosstab(temp_df.index, [ temp_df[i] for i in categoricalCols ])
    else:
        temp_df_num = temp_df[[*numericalCols]]
        temp_df = temp_df_num

    temp_df = temp_df.resample(resample).agg(get_aggr_func(agg))
    return temp_df

def get_aggregate_data(df, resample="", categoricalCols=[], numericalCols=[], pivot=False, agg="sum"):
    if not categoricalCols and not numericalCols:
        raise Exception("Either categoricalCols or numericalCols is required")

    data = {}

    temp_df = df[[*categoricalCols, *numericalCols]]
    if categoricalCols and numericalCols and not pivot:
        temp_df_cat = temp_df[[*categoricalCols]].groupby([temp_df.index] + categoricalCols).size().unstack(fill_value=0).reset_index()
        temp_df_num = temp_df[[*numericalCols]].reset_index()
        temp_df = temp_df_cat.merge(temp_df_num, left_index=True, right_index=True).agg(get_aggr_func(agg))
    elif categoricalCols and numericalCols and pivot:
        temp_df = temp_df.pivot_table(columns=categoricalCols, values=(numericalCols if len(numericalCols) > 1 else numericalCols[0]), fill_value=0).agg(get_aggr_func(agg))
    elif categoricalCols and not numericalCols:
        temp_df = temp_df[[*categoricalCols]].groupby([temp_df.index] + categoricalCols).size().unstack(fill_value=0).agg(get_aggr_func(agg))
    else:
        temp_df = temp_df.agg(get_aggr_func(agg))

    data["index"] = temp_df.index.tolist()
    data["values"] = temp_df.fillna(0).values.transpose().tolist()

    return data

def get_time_series_data(df, timeCol, resample="D", categoricalCols=[], numericalCols=[], timef="%Y-%m-%d", pivot=False, agg="sum"):
    data = {}

    temp_df = get_time_series_df(df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot, agg=agg)

    data["index"] = temp_df.index.strftime(timef).tolist()
    data["columns"] = temp_df.columns.tolist()
    data["values"] = temp_df.values.transpose().tolist()

    return data

def get_time_series_aggregate_data(df, timeCol, resample="", categoricalCols=[], numericalCols=[], pivot=False, agg="sum"):
    data = {}

    temp_df = get_time_series_df(df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot, agg=agg)
    temp_df = temp_df.agg(get_aggr_func(agg))

    data["index"] = temp_df.index.tolist()
    data["values"] = temp_df.fillna(0).values.transpose().tolist()

    return data

def get_exponential_smoothing_forecast_data(df, timeCol, resample="D", categoricalCols=[], numericalCols=[], timef="%Y-%m-%d", pivot=False, agg="sum"):
    data = {}

    temp_df = get_time_series_df(df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot, agg=agg)

    data = []
    for i in temp_df.columns:
        ts = TimeSeries.from_dataframe(temp_df[[i]])
        train, val = ts[:-30], ts[-30:]

        model = ExponentialSmoothing()
        model.fit(train)

        forecast = model.predict(90)
        data.append({
            "index": forecast.time_index.strftime(timef).tolist(),
            "columns": forecast.columns.tolist(),
            "values": forecast.values().transpose().tolist(),
            })

    return data

def get_prophet_forecast_data(df, timeCol, models, resample="D", categoricalCols=[], numericalCols=[], timef="%Y-%m-%d", pivot=False, agg="sum"):
    data = {}

    temp_df = get_time_series_df(df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot, agg=agg)

    loaded_models = []
    for i in models:
        if os.path.isfile(i["path"]):
            with open(i["path"], "rb") as file:
                loaded_models.append({ "model": pickle.load(file), "column": i["column"] })
        else:
            raise Exception("Model path is not a file or doesn't exists")

    data = []
    for m in loaded_models:
        future = m["model"].make_future_dataframe(periods=30)
        fcst_prophet_train = m["model"].predict(future)

        forecasted_df = fcst_prophet_train[fcst_prophet_train['ds'] >= temp_df.index[-1]]
        forecasted_df = forecasted_df[['ds', 'yhat']]

        data.append({
            "index": forecasted_df.ds.dt.strftime("%Y-%m-%d").tolist(),
            "columns": [m["column"]],
            "values": [forecasted_df.yhat.tolist()]
        })
    return data
