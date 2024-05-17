import pandas as pd
from darts.timeseries import TimeSeries
from darts.models import ExponentialSmoothing

def getCatNumData(df, timeCol, resample="D", categoricalCols=[], numericalCols=[], pivot=False):
    if not categoricalCols and not numericalCols:
        raise Exception("Either categoricalCols or numericalCols is required")

    temp_df = df[[*categoricalCols, *numericalCols, timeCol]].set_index(timeCol)

    if categoricalCols and numericalCols and not pivot:
        temp_df_cat = pd.crosstab(temp_df.index, [ temp_df[i] for i in categoricalCols ])
        temp_df_num = temp_df[[*numericalCols]]
        temp_df = temp_df_cat.merge(temp_df_num, left_index=True, right_index=True)
    elif categoricalCols and numericalCols and pivot:
        temp_df = temp_df.pivot_table(index=[timeCol], columns=categoricalCols, values=(numericalCols[0] if len(numericalCols) > 1 else numericalCols[0]), fill_value=0)
    elif categoricalCols and not numericalCols:
        temp_df = pd.crosstab(temp_df.index, [ temp_df[i] for i in categoricalCols ])
    else:
        temp_df_num = temp_df[[*numericalCols]]
        temp_df = temp_df_num

    temp_df = temp_df.resample(resample).sum()
    return temp_df

def getTimeSeriesData(df, timeCol, resample="D", categoricalCols=[], numericalCols=[], timef="%Y-%m-%d", pivot=False):
    data = {}

    temp_df = getCatNumData(df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot)

    data["index"] = temp_df.index.strftime(timef).tolist()
    data["columns"] = temp_df.columns.tolist()
    data["values"] = temp_df.values.transpose().tolist()

    return data

def getExponentialSmoothingForecastData(df, timeCol, resample="D", categoricalCols=[], numericalCols=[], timef="%Y-%m-%d", pivot=False):
    data = {}

    temp_df = getCatNumData(df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot)

    data = []
    for i in temp_df.columns:
        ts = TimeSeries.from_dataframe(temp_df[[i]])
        train, val = ts[:-30], ts[-30:]

        model = ExponentialSmoothing()
        model.fit(train)

        forecast = model.predict(90)
        data.append({
            "index": forecast.time_index.strftime("%Y-%m-%d").tolist(),
            "columns": forecast.columns.tolist(),
            "values": forecast.values().transpose().tolist(),
            })

    return data
