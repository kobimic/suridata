import concurrent.futures
import json
import os
import random
import itertools
from math import ceil

from typing import List, Dict, Tuple

EMPLOYEE_FILENAME = 'anak_data.json'
TOTAL_PROCESSES = os.getenv("TOTAL_PROCESSES", 2)


def read_employee_file(filename: str) -> List[Dict]:
    try:
        with open(filename) as fh:
            data = json.load(fh)
        return data
    except FileNotFoundError:
        raise Exception(f"Could not find {filename}")
    except Exception:
        raise Exception(f"Could not parse {filename}")


def filter_duplicates(employee_list: List) -> List[Dict]:
    filtered_list: List[Dict] = []
    filter_by_hash = set()

    for record in employee_list:
        hash_record = f"{record['name']}{record['department']}{record['age']}".lower()
        if hash_record not in filter_by_hash:
            filtered_list.append(record)
        filter_by_hash.add(hash_record)

    return filtered_list


def create_pairs(employee_list: List) -> List[Tuple[str, str]]:
    tuple_list: List[str] = []

    for i in range(len(employee_list) - 1):
        tuple_list.append((employee_list[i]['name'], employee_list[i + 1]['name']))
    return tuple_list


def task(employee_list: List) -> List[Tuple[str, str]]:
    random.shuffle(employee_list)
    paired_employees = create_pairs(employee_list)
    return paired_employees


def chunk_data(data, n):
    for i in range(0, len(data), n):
        yield data[i:i + n]


def merge_sub_process_results(employee_list: List):
    merged_list = []
    connections_list = []
    for i in range(len(employee_list) - 1):
        last_pair = employee_list[i][-1]
        first_pair = employee_list[i + 1][0]
        linked_pair = (last_pair[1], first_pair[0])
        connections_list.append(linked_pair)
    for i, sub_list in enumerate(employee_list):
        for pair in sub_list:
            merged_list.append(pair)

        if i < len(connections_list):
            merged_list.append(connections_list[i])

    if len(merged_list) > 2:
        merged_list.append((merged_list[-1][1], merged_list[0][0]))
    return list(merged_list)


def main():
    data = read_employee_file(EMPLOYEE_FILENAME)
    filtered_data = filter_duplicates(data)

    data_as_chunks = []
    for chunk in chunk_data(filtered_data, ceil(len(data)/TOTAL_PROCESSES)):
        data_as_chunks.append(chunk)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        processed_data = []
        for results in executor.map(task, data_as_chunks):
            processed_data.append(results)
    merged_data = merge_sub_process_results(processed_data)
    print(merged_data)


if __name__ == '__main__':
    main()
