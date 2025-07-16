import os
import zipfile
import gdown
from cnnClassifier.utils.common import get_size
from cnnClassifier import logger
from cnnClassifier.entity.config_entity import DataIngestionConfig

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self) -> str:
        """
        Downloads a file from a Google Drive URL and saves it locally.
        Returns the local file path.
        """
        try:
            dataset_url = self.config.source_URL
            zip_download_dir = self.config.local_data_file

            os.makedirs(os.path.dirname(zip_download_dir), exist_ok=True)
            logger.info(f"Downloading data from {dataset_url} into file {zip_download_dir}")

            file_id = dataset_url.split("/")[-2]
            prefix = 'https://drive.google.com/uc?export=download&id='
            gdown.download(prefix + file_id, zip_download_dir, quiet=False)

            logger.info(f"Downloaded data from {dataset_url} into file {zip_download_dir}")
            return zip_download_dir

        except Exception as e:
            logger.error(f"Error in downloading file: {e}")
            raise e

    def extract_zip_file(self) -> None:
        """
        Extracts the downloaded ZIP file into a specified directory.
        """
        try:
            unzip_path = self.config.unzip_dir
            os.makedirs(unzip_path, exist_ok=True)

            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_path)

            logger.info(f"Extracted zip file {self.config.local_data_file} to {unzip_path}")

        except Exception as e:
            logger.error(f"Error in extracting zip file: {e}")
            raise e
