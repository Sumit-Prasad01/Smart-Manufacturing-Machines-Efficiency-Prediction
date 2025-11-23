from src.custom_exception import CustomException
from src.logger import get_logger

from config.paths_config import *
from config.model_config import *
from config.data_ingestion_config import *

from src.data_ingestion import DataIngestion
from src.data_processing import DataProcessing
from src.model_training import ModelTraining


logger = get_logger(__name__)


if __name__ == "__main__":
    try:

        # Data Ingestion
        ingest = DataIngestion(DATASET_NAME, TARGET_DIR)
        ingest.run()

        # Data Processing
        processor = DataProcessing(RAW_DATA_PATH, PROCESSED_DATA_PATH)
        processor.run()

        # Model Training
        trainer = ModelTraining(MODEL_PATH)
        trainer.run()

    except Exception as e:
        logger.error(f"Pipeline terminated due to error: {e}")
        raise CustomException("Failed to run training pipeline", e)