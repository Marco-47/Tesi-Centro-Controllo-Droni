import redis
import time
from threading import Thread
import json
import logging

SEPARATOR = '*_*|*'
SEPARATOR2 = '__*__'


def send_command()-> None:
    r: redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    while (True):
        command: str = input()
        if "register_network" in command:
            data_ls: list = command.split(' ')
            if len(data_ls) == 3:
                try:
                    f = open(data_ls[2])
                    jsonF = json.load(f)
                except Exception as e:
                    logging.error(e)
                else:
                    #info_map = str(jsonF["square_cordinates"])
                    data: str = f"{data_ls[1]}{SEPARATOR}{str(jsonF)}"
                    r.lpush("queue:register_network", data)
            else:
                print(f"register_network takes 2 arguments but given {len(data_ls)-1}")
        elif "show_map" in command:
            data_ls: list = command.split(' ')
            if len(data_ls) == 3:
                t: Thread = Thread(target=send_command, args=(data_ls[1],data_ls[2]))
                t.start()
            else:
                 print(f"show_map takes 2 arguments but given {len(data_ls)-1}")
        else:
            print("command not found!")




if __name__ == "__main__":
    send_command()



'''def create__chessboard(col1: Tuple[int, int, int], col2: Tuple[int, int, int])-> None:
    array = np.zeros([360, 360, 3], dtype=np.uint8)
     
    map = {}
    pos = []
    count = 1
    flag = 0
    
    for x in range(360):
        for y in range(360):
            if ((y//90,x//90) not in pos):
                pos.append((y//90,x//90))
                map[str(count)] = str((y,x))
                count+=1
            if (x // 90) % 2 !=  (y // 90) % 2:    
                array[y][x]  = col2
            else:
                array[y][x] = col1
    
    
    
    img = Image.fromarray(array)
    img.save('rete1.png')
    print(map)
    return'''
