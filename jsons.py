import os
import json

FILES = {
    'path_to_astro_cities_list': os.getcwd() + "\\data\\Astro_cities.json",
    'path_to_priority_cities_file': os.getcwd() + "\\data\\all_priority_cities.json",
    'path_to_ports_file': os.getcwd() + "\\data\\ports.json"
}


def write_to_file(content, path):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            print(f"ERROR: ОШИБКА ЗАПИСИ В ФАЙЛ {exc}")

    with open(path, 'w') as json_file:
        json.dump(content, json_file, indent=2)


def read_from_file(path):
    if os.path.exists(path) is False:
        write_to_file([], path)
        read_from_file(path)

    with open(path, 'r') as json_file:
        return json.load(json_file)


def get_by_index(path, index):
    json_content = read_from_file(path)
    return json_content[index]


def get_by_attribute_value(path, attr, val):
    content = read_from_file(path)
    for json_content in content:
        if json_content[attr] == val:
            return json_content


def get_like_pages(content, get_after_index=0, limit=3):
    data = []
    get_after_index *= limit
    content_count = len(content)
    iterator = 0

    is_limit_correct_val = content_count - get_after_index
    if is_limit_correct_val >= limit:
        limit -= 1

    if get_after_index is not 0:
        for i in range(get_after_index, content_count):
            data.append(content[i])
            if iterator == limit:
                return data
            else:
                iterator += 1
    else:
        for i in range(0, content_count):
            data.append(content[i])
            if iterator == limit:
                return data
            else:
                iterator += 1
