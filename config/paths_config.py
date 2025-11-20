import os


RAW_DATA_PATH = "artifacts/raw/data.csv"
PROCESSED_DATA_PATH = "artifacts/processed/"

# Features Path
## Save 
X_TRAIN_PATH = os.path.join(PROCESSED_DATA_PATH, "X_train.pkl")
X_TEST_PATH = os.path.join(PROCESSED_DATA_PATH, "X_test.pkl")
y_TRAIN_PATH = os.path.join(PROCESSED_DATA_PATH, "y_train.pkl")
y_TEST_PATH = os.path.join(PROCESSED_DATA_PATH, "y_test.pkl")

# load path
X_TRAIN_LOAD_PATH = 'artifacts/processed/X_train.pkl'
X_TEST_LOAD_PATH = 'artifacts/processed/X_test.pkl'
y_TRAIN_LOAD_PATH = 'artifacts/processed/y_train.pkl'
y_TEST_LOAD_PATH = 'artifacts/processed/y_test.pkl'

# Model Training
MODEL_PATH = "artifacts/models"
os.makedirs(MODEL_PATH, exist_ok=True)
SAVE_MODEL_PATH = os.path.join(MODEL_PATH, "xgb_model.pkl")
SAVED_MODEL_PATH = "artifacts/models/xgb_model.pkl"

# Visuals Path
VISUALS_PATH = "artifacts/visuals"
os.makedirs(VISUALS_PATH, exist_ok = True)