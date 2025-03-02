import os
from datastore.file import read_dataset_pickle
from computes.preprocess import preprocess_dataset
import config

dataset_path = os.path.join(os.getcwd(), config.DATASET_PATH)
dataset = read_dataset_pickle([dataset_path])[0]
dataset = preprocess_dataset(dataset)
