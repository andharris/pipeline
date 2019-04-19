import json
import math
import multiprocessing
import re
import time
from dataclasses import dataclass, fields
from itertools import chain, islice


# Parse json file
def read_json(filename):
    with open(filename) as f:
        for line in f:
            yield json.loads(line)


# Schema defintion and validation
@dataclass
class LogEntry:
    type: str

    def __post_init__(self):
        field_names = {field.name for field in fields(self)}
        field_names_typed = {
            field.name
            for field in fields(self)
            if isinstance(self.__dict__[field.name], field.type)
        }
        if field_names != field_names_typed:
            raise TypeError


@dataclass
class LogString(LogEntry):
    string: str


@dataclass
class LogNumber(LogEntry):
    number: int


def validation(DataClass, log_entry):
    try:
        DataClass(**log_entry)
        return True
    except:
        return False


def valid_log(log_entry):
    return validation(LogString, log_entry) or validation(LogNumber, log_entry)


# Transformation
def transform_entry(log_entry):
    if "number" in log_entry:
        log_entry["number"] = math.log(log_entry["number"])

    if "string" in log_entry:
        log_entry["string"] = log_entry["string"] + "-improved!"

    return log_entry


# Simulate database insert
def save_into_database(batch):
    return len(list(batch))


def partition_all(iterable, n):
    for first in iterable:
        yield chain([first], islice(iterable, n - 1))


def keep(log_entry):
    if log_entry.get("number", 0) > 900:
        return True

    if re.search("a", log_entry.get("string", "")):
        return True

    return False


# Process json file
def process_file(filename):
    processed_records = 0
    data = read_json(filename)
    valid_entries = (entry for entry in data if valid_log(entry))
    transformed_entries = (
        transform_entry(entry)
        for entry in valid_entries
        if keep(entry)
    )
    batches = partition_all(transformed_entries, 1000)
    for batch in batches:
        processed_records += save_into_database(batch)
    return processed_records


def process_lazy(files):
    processed_records = 0
    for f in files:
        processed_records += process_file(f)
    return processed_records


def process_parallel(files):
    n_works = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(n_works)
    pool.map(process_file, files)


def main():
    print("--- Time how we do for a single file ---")
    print("\nLazy:")
    start = time.time()
    processed_records = process_lazy(["data/dummy.json", ])
    end = time.time()
    print(f"Elapsed time: {(end - start) * 1000} msecs")


    print("\nParallel:")
    start = time.time()
    processed_records = process_parallel(["data/dummy.json", ])
    end = time.time()
    print(f"Elapsed time: {(end - start) * 1000} msecs")

    print("\n--- What if we run on 10 files ---")
    print("\nLazy:")
    start = time.time()
    processed_records = process_lazy(["data/dummy.json", ] * 10)
    end = time.time()
    print(f"Elapsed time: {(end - start) * 1000} msecs")


    print("\nParallel:")
    start = time.time()
    processed_records = process_parallel(["data/dummy.json", ] * 10)
    end = time.time()
    print(f"Elapsed time: {(end - start) * 1000} msecs")


if __name__ == "__main__":
    main()
