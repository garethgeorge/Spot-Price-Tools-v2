from os import path 
import argparse 
import gzip 
from datetime import datetime
from tqdm import tqdm
import sqlite3 
from lib.time_util import time_to_epoch
from lib.dataset import Dataset

def open_file(filename, mode="r"):
    if filename.endswith(".gz"):
        return gzip.open(filename, mode)
    else:
        return open(filename, mode)

def main():
    parser = argparse.ArgumentParser(description="Import dataset into a useful representation that we can use")
    parser.add_argument("dataset_file", help="the file to import")
    parser.add_argument("data_directory", help="the location where we are storing the data.")
    args = parser.parse_args()
    print("processing file: " + args.dataset_file)
    if not path.exists(args.dataset_file):
        raise Exception("no such file: " + args.dataset_file)

    dataset_file, data_directory = open_file(args.dataset_file), args.data_directory

    ts_remove_str_len = -len(".000Z")

    with Dataset(data_directory) as dataset:
        for line in tqdm(dataset_file):
            try:
                if type(line) != str: 
                    line = line.decode("ascii")
                if len(line) == 0: continue 
                _, az, insttype, os, price, timestamp_str = tuple(line.strip().split("\t"))
                price = float(price)
                timestamp = datetime.strptime(timestamp_str[:ts_remove_str_len], "%Y-%m-%dT%H:%M:%S")
                dataset.insert_data(az, insttype, time_to_epoch(timestamp), price)
            except Exception as e:
                print(e)

main()