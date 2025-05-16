import os
from datastore.file import read_pickle, save_dataset_as_pickle
from sources.file import read_dataset
from computes.preprocess import PreprocessPandas
import config

if os.path.isfile(config.DATASTORE_FILE_PATH):
    dataset = read_pickle(config.DATASTORE_FILE_PATH)
else:
    dataset = read_dataset(config.INIT_SOURCE_PATH)
    save_dataset_as_pickle(dataset, config.DATASTORE_FILE_PATH)

preprocessor = PreprocessPandas()
dataset = preprocessor.preprocess_dataset(dataset)
