import pandas as pd
from computes.preprocess import PreprocessPolars, PreprocessPandas
from computes.prophet import predict
from computes.timeseries_analysis import build_arima_model, bayesian_optimization_arima_predict

def test_preprocess_polars_filter_cols(test_df_dummy):
    ds_pl, ds_pd = test_df_dummy

    preprocess = PreprocessPolars()
    preprocess.filter_cols(ds_pl)

def test_preprocess_polars_convert_dtypes(test_df_dummy):
    ds_pl, ds_pd = test_df_dummy

    preprocess = PreprocessPolars()
    preprocess.convert_dtypes(ds_pl)

def test_preprocess_polars(test_df_dummy):
    ds_pl, ds_pd = test_df_dummy

    preprocess = PreprocessPolars()
    preprocess.preprocess_dataset(ds_pl)

def test_preprocess_pandas_convert_dtypes(test_df_dummy):
    ds_pl, ds_pd = test_df_dummy

    preprocess = PreprocessPandas()
    preprocess.convert_dtypes(ds_pd)

def test_preprocess_pandas(test_df_dummy):
    ds_pl, ds_pd = test_df_dummy

    preprocess = PreprocessPandas()
    preprocess.preprocess_dataset(ds_pd)

def test_prophet_polars(test_df_seasonal):
    ds_pl, ds_pd = test_df_seasonal
    prediction = predict(ds_pl, periods=10, ds_col="ds", y_col="y")

def test_build_arima_model(test_df_seasonal):
    ds_pl, ds_pd = test_df_seasonal
    aic = build_arima_model(pd.Series(ds_pd["y"], index=ds_pd["ds"]), p=1, d=0, q=1)

def test_bayesian_optimization_arima_predict(test_df_seasonal):
    ds_pl, ds_pd = test_df_seasonal
    prediction = bayesian_optimization_arima_predict(ds_pd, periods=10, ds_col="ds", y_col="y")
