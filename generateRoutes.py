from flask import request
from filterdf import filtertime
from getdata import getTimeSeriesData, getExponentialSmoothingForecastData, getProphetForecastData

def type_is_true(value):
    return value.lower() == "true"

def generate_route_callback(name, df, timeCol, categoricalCols=[], numericalCols=[], models=[]):
    def callback():
        tipe_data = request.args.get("tipe_data")
        timeseries = request.args.get("timeseries", type=type_is_true)
        forecast = request.args.get("forecast", type=type_is_true)
        pivot = request.args.get("pivot", type=bool)
        tahun = request.args.get("tahun", type=int)
        bulan = request.args.get("bulan", type=int)
        relative_time = request.args.get("relative_time", type=str)
        start_date = request.args.get("start_date", type=str)
        end_date = request.args.get("end_date", type=str)
        resample = request.args.get("resample", type=str, default="D")
        timef = request.args.get("timef", type=str)

        #temp_df = df[[timeCol, *categoricalCols, *numericalCols, "provinsi"]]
        #temp_df = temp_df.loc[temp_df["provinsi"] == "RIAU"]

        temp_df = df[[timeCol, *categoricalCols, *numericalCols]]
        temp_df = filtertime(temp_df, timeCol, month=bulan, year=tahun, relative_time=relative_time, start_date=start_date, end_date=end_date)

        data = {}
        if not timeseries and not forecast:
            # TODO: handle multiple columns if needed. This code currently only
            # returns the first columns of the given categoricalCols. The
            # numericalCols is ignored
            if not categoricalCols:
                raise Exception("Only one categoricalCols is supported")
            data["index"] = temp_df[categoricalCols[0]].value_counts().index.values.tolist()
            data["values"] = temp_df[categoricalCols[0]].value_counts().values.tolist()
            return data

        if timeseries:
            return getTimeSeriesData(temp_df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot)

        if forecast:
            if not models:
                return getExponentialSmoothingForecastData(temp_df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot)
            else:
                return getProphetForecastData(temp_df, timeCol, models=models, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot)


        return data

    callback.__name__ = name
    return callback

def init_routes_data(app, routes):
    for r in routes:
        app.route(f"/api/data{r['route']}", methods=["GET"])(r['callback'])
    print("Data routes initialized")
    return app
