import xgboost as xgb
from sklearn.model_selection import RandomizedSearchCV


MODEL = xgb.XGBClassifier(
    objective="binary:logistic",
    eval_metric="logloss",
    tree_method="hist",
    use_label_encoder=False
)


PARAM_GRID = {
    "n_estimators": [200, 300, 400, 500],
    "max_depth": [4, 6, 8, 10],
    "learning_rate": [0.01, 0.03, 0.05, 0.1],
    "subsample": [0.6, 0.8, 1.0],
    "colsample_bytree": [0.6, 0.8, 1.0],
    "gamma": [0, 0.1, 0.2, 0.3],           # minimum loss reduction
    "min_child_weight": [1, 3, 5],         # controls leaf node size
    "reg_alpha": [0, 0.1, 0.5],            # L1 regularization
    "reg_lambda": [1, 1.5, 2],             # L2 regularization
}


SEARCH = RandomizedSearchCV(
    estimator = MODEL ,
    param_distributions= PARAM_GRID,
    n_iter = 30,         # search size (increase for better results)
    scoring = "accuracy",
    cv = 5,
    verbose = 2,
    n_jobs = -1
)


