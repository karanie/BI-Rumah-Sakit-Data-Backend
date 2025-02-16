import os

ALLOWED_EXTENSIONS = os.environ.get("ALLOWED_EXTENSIONS", "csv;xlsx").split(";")
DATASET_PATH = os.environ.get("DATASET_PATH", "data/dataset/DC1")
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "data/dataset/uploaded/")
DB_TABLE = os.environ.get("DB_TABLE", "dataset")
DB_CONNECTION = os.environ.get("DB_CONNECTION", ":memory:")
