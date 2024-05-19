import json
import os
import random
from typing import List, Dict, Tuple

EMPLOYEE_FILENAME = 'anak_data.json'


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

    if len(employee_list) > 2:
        tuple_list.append((employee_list[-1]['name'], employee_list[0]['name']))
    return tuple_list


def main():
    data = read_employee_file(EMPLOYEE_FILENAME)

    filtered_data = filter_duplicates(data)
    # random.shuffle(filtered_data)
    paired_employees = create_pairs(filtered_data)

    print(paired_employees)


if __name__ == '__main__':
    main()
