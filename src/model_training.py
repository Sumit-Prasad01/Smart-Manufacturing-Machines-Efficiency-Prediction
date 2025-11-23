import os
import numpy as np
import joblib
import xgboost as xgb
from src.custom_exception import CustomException
from src.logger import get_logger
from utils.helpers import Loader
from config.model_config import *
from config.paths_config import *
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns


logger = get_logger(__name__)

class ModelTraining:

    def __init__(self, output_path):
        
        self.output_path : str = output_path
        self.model = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None

        logger.info("Model training initialized.")

    
    def load_data(self):
        try:
            self.X_train, self.X_test, self.y_train, self.y_test = Loader.load_processed_data(X_TRAIN_LOAD_PATH, X_TEST_LOAD_PATH, y_TRAIN_LOAD_PATH, y_TEST_LOAD_PATH)

            logger.info("Data loaded successfully.")
        
        except Exception as e:
            logger.error(f"Failed to load processed data: {e}")
            raise Exception(f"Error while loading processed data.",e)
        
    
    def train_model(self):

        try:

            logger.info("Model training started.")

            SEARCH.fit(self.X_train, self.y_train)

            
            self.model = SEARCH.best_estimator_

            
            joblib.dump(self.model, SAVE_MODEL_PATH)

            logger.info("Best Parameters Found:", SEARCH.best_params_)
            logger.info("Model saved at:", SAVED_MODEL_PATH)

            logger.info("Model training and saving completed successfully.")

        
        except Exception as e:
            logger.error(f"Failed to train and save model: {e}")
            raise Exception(f"Error while training and saving model: ",e)
    

    def evaluate_model(self):
        try:
            logger.info("Evaluating Our Model")

            y_pred = self.model.predict(self.X_test)

            accuracy = accuracy_score(self.y_test, y_pred)
            precision = precision_score(self.y_test, y_pred, average = "weighted")
            recall = recall_score(self.y_test, y_pred, average = "weighted")
            f1score = f1_score(self.y_test, y_pred, average = "weighted")

            logger.info(f"Accuracy Score :{accuracy}")
            logger.info(f"Precision Score :{precision}")
            logger.info(f"Recall Score :{recall}")
            logger.info(f"F1 Score Score :{f1score}")


            cm = confusion_matrix(self.y_test, y_pred)
            plt.figure(figsize=(8, 6))
            sns.heatmap(cm, annot = True, cmap = "Blues", xticklabels = np.unique(self.y_test), yticklabels = np.unique(self.y_test))
            plt.title("Confusion Matrix")
            plt.xlabel("Predicted labels")
            plt.ylabel("Actual labels labels")
            plt.savefig(f"{VISUALS_PATH}/Confusion_Matrix.png")
            plt.close()

            logger.info("Confusion Matrix saved successfully.")

        
        except Exception as e:
            logger.error(f"Error During Evaluating model {e}")
            raise CustomException("Failed to Evaluate model",e)
    

    def run(self):
        try:
            logger.info("Starting model training pipeline")

            self.load_data()
            self.train_model()
            self.evaluate_model()

            logger.info("Model training completed successfully.")

        except Exception as e:
            logger.error(f"Error while running model training pipeline. {e}")
            raise CustomException("Failed to run model training pipeline: ",e)



if __name__ == "__main__":

    trainer = ModelTraining(MODEL_PATH)
    trainer.run()