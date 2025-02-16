from utils.colddata import read_dataset_pickle
from utils.preprocess import preprocess_dataset

dataset_path = "data/dataset/DC1"
dataset = read_dataset_pickle([dataset_path])[0]
dataset = preprocess_dataset(dataset)
