import json
from flask import request
from flask_caching import Cache
from filterdf import filtertime, filtercols
from getdata import get_aggregate_data, get_time_series_data, get_time_series_aggregate_data, get_exponential_smoothing_forecast_data, get_prophet_forecast_data

def type_is_true(value):
    return value.lower() == "true"

def generate_route_callback(name, df, timeCol, categoricalCols=[], numericalCols=[], models=[]):
    def callback():
        tipe_data = request.args.get("tipe_data")
        aggregate = request.args.get("aggregate", type=type_is_true)
        timeseries = request.args.get("timeseries", type=type_is_true)
        forecast = request.args.get("forecast", type=type_is_true)
        pivot = request.args.get("pivot", type=bool)
        filters = json.loads(request.args.get("filters", type=str, default="{}"))
        tahun = request.args.get("tahun", type=int)
        bulan = request.args.get("bulan", type=int)
        relative_time = request.args.get("relative_time", type=str)
        start_date = request.args.get("start_date", type=str)
        end_date = request.args.get("end_date", type=str)
        resample = request.args.get("resample", type=str, default="D")
        agg = request.args.get("agg", type=str, default="sum")
        timef = request.args.get("timef", type=str)

        temp_df = df[[timeCol, *{*categoricalCols, *numericalCols, *filters.keys()}]]
        temp_df = filtertime(temp_df, timeCol, month=bulan, year=tahun, relative_time=relative_time, start_date=start_date, end_date=end_date)
        temp_df = filtercols(temp_df, filters)
        temp_df = temp_df[[timeCol, *categoricalCols, *numericalCols]]

        data = {}

        if aggregate and timeseries:
            return get_time_series_aggregate_data(temp_df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot, agg=agg)

        if aggregate:
            return get_aggregate_data(temp_df, categoricalCols=categoricalCols, numericalCols=numericalCols, pivot=pivot, agg=agg)

        if timeseries:
            return get_time_series_data(temp_df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot, agg=agg)

        if forecast:
            if not models:
                return get_exponential_smoothing_forecast_data(temp_df, timeCol, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot, agg=agg)
            else:
                return get_prophet_forecast_data(temp_df, timeCol, models=models, resample=resample, categoricalCols=categoricalCols, numericalCols=numericalCols, timef=timef, pivot=pivot, agg=agg)

        temp_df = temp_df.set_index(timeCol)
        data["index"] = temp_df.index.strftime(timef).tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.fillna(0).values.transpose().tolist()
        return data

    callback.__name__ = name
    return callback

def init_routes_data(app, routes):
    cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
    cache.init_app(app)

    for r in routes:
        app.route(f"/api/data{r['route']}", methods=["GET"])(cache.cached(timeout=50, query_string=True)(r['callback']))
    print("Data routes initialized")
    return app
