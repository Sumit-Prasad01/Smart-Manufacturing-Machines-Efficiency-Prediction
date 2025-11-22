import os
import joblib
import numpy as np
import pandas as pd

from config.paths_config import *
from src.logger import get_logger
from utils.helpers import Loader
from src.custom_exception import CustomException

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split


logger = get_logger(__name__)

class DataProcessing:

    def __init__(self, input_path : str, output_path : str):
        
        self.input_path = input_path
        self.output_path = output_path
        self.df = None
        self.features = None

        os.makedirs(self.output_path, exist_ok = True)

        logger.info("Data processing initialized.")


    def preprocess(self):
        try:

            self.df = Loader.load_data(self.input_path)
            self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'], errors = 'coerce')
            categorical_columns = ['Operation_Mode', 'Efficiency_Status']

            for col in categorical_columns:
                self.df[col] = self.df[col].astype('category')

            self.df["Year"] = self.df["Timestamp"].dt.year
            self.df["Month"] = self.df["Timestamp"].dt.month
            self.df["Day"] = self.df["Timestamp"].dt.day
            self.df["Hour"] = self.df["Timestamp"].dt.hour

            self.df.drop(columns = ['Timestamp', 'Machine_ID'], inplace = True)

            columns_to_encode = ["Efficiency_Status", "Operation_Mode"]

            for col in columns_to_encode:
                le = LabelEncoder()
                self.df[col] = le.fit_transform(self.df[col])

            logger.info("All basic data processing done.")

        except Exception as e:
                logger.error(f"Error while processing data.")
                raise CustomException(f"Failed to process data.", e)

    def split_and_scale_and_save(self):
        try:

            self.features = [
                    'Operation_Mode', 'Temperature_C', 'Vibration_Hz',
                    'Power_Consumption_kW', 'Network_Latency_ms', 'Packet_Loss_%',
                    'Quality_Control_Defect_Rate_%', 'Production_Speed_units_per_hr',
                    'Predictive_Maintenance_Score', 'Error_Rate_%','Year', 'Month', 'Day', 'Hour'
                ]

            X = self.df[self.features]
            y = self.df['Efficiency_Status']

            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size = 0.2, random_state = 42, stratify = y)

            joblib.dump(X_train, X_TRAIN_PATH)
            joblib.dump(X_test, X_TEST_PATH)
            joblib.dump(y_train,y_TRAIN_PATH)
            joblib.dump(y_test, y_TEST_PATH)

            joblib.dump(scaler, SCALER_PATH)

            logger.info("Data scaled, splitted and saved data successfully.")
        

        except Exception as e:
            logger.error(f"Error while scaling,  splitting and saving data.")
            raise CustomException(f"Failed to scale, split and save data.", e)


    def run(self):
        try:
            logger.info("Data processing pipeline started.")

            self.preprocess()
            self.split_and_scale_and_save()

            logger.info("Data processing pipeline executed successfully.")
        
        except Exception as e:
            logger.error(f"Error while running data processing pipeline.")
            raise CustomException(f"Failed to run data processing pipeline.", e)
        

if __name__ == "__main__":

    processor = DataProcessing(RAW_DATA_PATH, PROCESSED_DATA_PATH)
    processor.run()