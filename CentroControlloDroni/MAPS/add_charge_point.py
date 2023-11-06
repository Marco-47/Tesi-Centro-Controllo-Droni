import random
import json
import sys
from ast import literal_eval



def add_charge_point(map_info: dict, num_area: int, num_charge_point: int):
    ls_node = list(range(num_area))
    added_area: set = set()
    for i in range(num_area,num_area+ num_charge_point):
        area_index = random.randint(0, len(ls_node)-1)
        new_area = ls_node[area_index]
        map_info[str(new_area)].append([i, 1])
        map_info[str(i)] = [new_area, 1]
        del ls_node[area_index]
        added_area.add(i)
    
    return str(added_area)


if __name__ == "__main__":
    net_id = sys.argv[1]
    num_area = sys.argv[2]
    charge_point = sys.argv[3]
    
    f = open(f"./{net_id}.json")
    map_info = str(json.load(f))
    map_info: dict = literal_eval(map_info)
    add_charge_point(map_info, int(num_area), int(charge_point))
    