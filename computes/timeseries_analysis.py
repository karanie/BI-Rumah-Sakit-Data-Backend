import pandas as pd
import sys

def build_arima_model(endog, p, d, q, exog=None):
    from statsmodels.tsa.arima.model import ARIMA
    try:
        model = ARIMA(endog, order=(p, d, q), exog=exog)
        model_fit = model.fit() # disp=False to suppress verbose output during fitting
        return model_fit, model_fit.aic
    except Exception as e:
        print(f"ARIMA({p},{d},{q}) failed: {e}")
        return None, sys.maxsize

def bayesian_optimization_arima_orders(df, ds_col="ds", y_col="y"):
    def arimaobj(p, d, q):
        _, aic = build_arima_model(pd.Series(df[y_col], index=df[ds_col]), p, d, q)
        return -aic
    pbounds = {'p': (0, 18, int), 'd': (0, 2, int), 'q': (0, 18, int)}
    print("Running BayesianOptimization")
    from bayes_opt import BayesianOptimization
    optimizer = BayesianOptimization(
        f=arimaobj,
        pbounds=pbounds,
        random_state=1,
    )
    optimizer.maximize(
        init_points=2,
        n_iter=2,
    )
    return optimizer.max["params"]["p"], optimizer.max["params"]["d"], optimizer.max["params"]["q"], optimizer.max["target"]

def bayesian_optimization_arima_predict(df, periods, ds_col="ds", y_col="y"):
    best_order = bayesian_optimization_arima_orders(df, ds_col, y_col)
    from statsmodels.tsa.arima.model import ARIMA
    final_model = ARIMA(pd.Series(df[y_col], index=df[ds_col]), order=(int(best_order[0]), int(best_order[1]), int(best_order[2])))
    final_model_fit = final_model.fit()

    forecast_steps = 20
    forecast_result = final_model_fit.forecast(steps=forecast_steps)

    return forecast_result
