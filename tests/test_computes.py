from computes.preprocess import PreprocessPolars, PreprocessPandas
from computes.prophet import predict

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
