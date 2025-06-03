import os

ALLOWED_EXTENSIONS = os.environ.get("ALLOWED_EXTENSIONS", "csv;xlsx").split(";")
INIT_SOURCE_PATH = os.path.join(os.getcwd(), os.environ.get("INIT_SOURCE_PATH", "data/dataset/DC1.csv.gz"))
DATASTORE_FILE_PATH = os.path.join(os.getcwd(), os.environ.get("DATASTORE_FILE_PATH", "data/dataset/DC1.pkl.gz"))
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "data/dataset/uploaded/")
DB_TABLE = os.environ.get("DB_TABLE", "dataset")
DB_CONNECTION = os.environ.get("DB_CONNECTION", "sqlite://")
PORT = os.environ.get("PORT", "5000")
