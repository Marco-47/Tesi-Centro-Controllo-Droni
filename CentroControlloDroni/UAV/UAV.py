'''Modello drone utilizzato per la simulazione'''

# Import delle librerie necessarie
from __future__ import annotations
import datetime
import json
import logging
import queue
import socket
import time
from ast import literal_eval
from dataclasses import dataclass, field
from threading import Thread
from typing import Tuple
import sys
import redis
import numpy as np
import redis
from numpy import asarray

# Definizione delle costanti
HOST = "127.0.0.1"  # Il nome host o l'indirizzo IP del server
PORT = 9000  # La porta utilizzata dal server
NET_ID = "rete1"
SEPARATOR = '*_*|*'
SEPARATOR2 = '__*__'

# Definizione di alcune variabili globali
ID: int
MAX_CLOCK_TIME: float
clock_time: float = 0.0
START_AREA: str
r: redis.Redis = redis.StrictRedis(host='localhost', port=6379, db=0)
flag_first_time = 0

# Definizione della classe UAV (drone)
@dataclass(slots=True)
class UAV:
    _network_id : str
    _id : str
    _map_info : dict
    state : str
    sock : socket
    battery: int[0:100]
    position: int
        
    def __get_id__(self)-> int:
        return self._id

    def __get_net_id__(self)-> int:
        return self._network_id

    def __get_info_area__(self, area: str)-> list:
        return self._map_info[area]
    
    def __get_state__(self)-> str:
        return self.state
    
    def __set_state__(self, state: str)-> None:
        self.state = state
    
    def __get_position__(self)-> str:
        return self.position

    def __get_uav_info__(self)-> list[int,str,float]:
        return [self._id, self.status, self.x, self.y, self.z]

    def __set_position__(self, position: int)-> None:
        self.position = position 
        
    def __get_battery__(self)-> float:
        return self.battery
    
    def __update_lvl_battery__(self, lvl_change : float)-> float:
        self.battery = self.battery + lvl_change
        return self.battery

# Inizializza un drone
def init_UAV(net_id: str, uav_id:int, position: int, battery: int[0:100], host: str, port: int)-> Tuple[socket.socket, UAV]:
    s: socket = create_socket_and_connect(host,port)
    map_info: dict
    uav: UAV = UAV(net_id, uav_id ,None,"wait_registration",s, battery, position)
    map_info = subscribe_UAV_to_network(uav)
    uav._map_info = literal_eval(map_info)
    uav.__set_state__("registered")
    return s, uav

# Crea un socket e si connette al server
def create_socket_and_connect(host: str, port: int) -> socket: 
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host,port))
    except socket.error as e:
        logging.error(f"error: {e} | function: create_socket_and_connect()")
        exit(1)
    return client_socket

# Sottoscrive il drone alla rete e riceve le informazioni dell'area
def subscribe_UAV_to_network(uav: UAV)-> Tuple[str, str]:
    map_info: str
    try:
        send_msg(uav, "register")
        map_info = rcv_msg(uav, 10142)[0]
    except socket.error as e:
        logging.error(f"error: {e} | function: subscribe_UAV_to_network()")
        return '-1'
    return map_info

# Ottiene un'area di lavoro per il drone
def get_work(uav: UAV)-> str:
    msg = f"wait_work{SEPARATOR}{uav.battery}"
    
    if send_msg(uav, msg) == 0:
        area_type, path_str = rcv_msg(uav, 1024)
        return area_type, literal_eval(path_str)
    else:
        logging.error(f"get_work --> send_work = -1")
        exit(1)

# Invia un messaggio al drone
def send_msg( uav: UAV, msg: str):
    global clock_time
    data = f"{clock_time}{SEPARATOR}{clock_time}{SEPARATOR}{uav.__get_net_id__()}{SEPARATOR}{uav.__get_id__()}{SEPARATOR}{uav.__get_position__()}{SEPARATOR}{msg}\0"
    try:
        uav.sock.send(bytes(data, encoding="utf-8"))
    except socket.error as e:
        logging.error(f"error: {e} | function: send_msg()")
        return '-1'
    return 0

# Riceve un messaggio dal drone
def rcv_msg(uav: UAV, lenght_msg: int)-> list[bytes]: 
    global clock_time
    msg = ''
    while True:
        data = uav.sock.recv(1024)
        msg += data.decode('utf-8')
        if len(data) < 1024:
            break
    
    msg = msg.split(SEPARATOR2)
    start_wait = clock_time
    update_clock(uav, float(msg[0]) + 1.0)
    total_wait = clock_time - start_wait
    r.hincrbyfloat('attesa_droni_totale','attesa', total_wait)
    r.hincrbyfloat('attesa_droni_totale','num_msg', 1)
    if total_wait > 15 and uav.state != '':
        r.hset('error_wait',f'drone{uav._id}', f'{clock_time}_{total_wait}')
    if clock_time >= MAX_CLOCK_TIME:
        exit(0)
    return msg[1:]

# Aggiorna lo stato del drone
def update_state_drone(u: UAV, new_state: str):
    global clock_time
    update_clock(u, clock_time + 0.1)
    u.__set_state__(f"{new_state}")
    r.lpush(f"log_test:info_drone:ID={u._id}:log_state", f"{clock_time},{u.__get_state__()}")
    send_msg(u, f"update_state{SEPARATOR}{new_state}")
    return

# Aggiorna lo stato dell'area
def update_state_area(u: UAV, update_type: str):
    global clock_time
    update_clock(u, clock_time + 0.1)
    send_msg(u, f"update_area{SEPARATOR}{update_type}")
    return

# Carica il drone
def charge(u: UAV):
    global clock_time
    update_state_drone(u, "charging")
    time_charge_needed: float = round((100 - u.__get_battery__()) / 0.17, 1) + 0.1
    if time_charge_needed < 0:
        logging.error("charge minore")
        exit(0)
    update_clock(u, clock_time + time_charge_needed)
    if u.battery < 99.9:
        logging.error("errore ricarica batteria")
        exit(1)
    return

# Effettua la sorveglianza
def survillance(u: UAV)-> None:
    global clock_time
    update_state_drone(u, "surveilling")
    update_clock(u, clock_time + 300.0)

# Avvia il drone
def uav_start(u: UAV)-> None:
    while True: 
        
        update_state_drone(u, "waiting")
       
        area_type, path = get_work(u)
        path = path[1:]
        
        update_state_drone(u, "moving_to_designed_position")
        
        for area in path:
            if (area != u.__get_position__()):
                move_to_area(u, area)
                
        update_state_area(u, "start_task")
        
        if area_type == '0':
            charge(u)
        else:
            survillance(u)
            
        update_state_area(u, "task_completed")

# Aggiorna il clock del drone
def update_clock(uav: UAV, new_clock: float):
    global clock_time
    if new_clock < clock_time:
        logging.error(f"new clock time: {new_clock} < clock time {clock_time}")
        exit(1)
    if new_clock >= MAX_CLOCK_TIME:
        new_clock = MAX_CLOCK_TIME
        
    update_battery(uav, new_clock - clock_time) 
    clock_time = round(new_clock, 1)
    
    if clock_time == MAX_CLOCK_TIME:
        send_msg(uav, "kill")
        rcv_msg(uav, 100)
        logging.error("killato correttamente")
        exit(0)
    return

# Aggiorna il livello della batteria del drone
def update_battery(uav: UAV, time: float):
    global r, clock_time
    battery_change: float = 0.0
    uav_state: str = uav.__get_state__()
    battery_start = uav.battery
    clock_start = clock_time
    change = 0
    
    if uav_state  == "waiting":
        battery_change = -0.0167 * time
        change = -0.0167
    elif uav_state == "surveilling":
        battery_change = -0.03 * time
        change = -0.03
    elif uav_state == "moving_to_designed_position":
        battery_change = -0.045 * time
        change = -0.045
    elif uav_state == "charging":
        battery_change = 0.17 * time
        change = 0.17
    
    uav.__update_lvl_battery__(battery_change)

    for _ in range(round(time)):
        battery_start += change
        clock_start += 1
        r.lpush(f"log_test:info_drone:ID={uav._id}:battery_log", f"{clock_start},  {round(battery_start, 1)}")
        if battery_start <= 0 or battery_start >= 100:
            break
    
    if uav.__get_battery__() <= 0.0:
        clock_time = clock_start
        uav.battery = 0.0
        send_msg(uav, f"disconnect{SEPARATOR}battery_dead")
        logging.error(f"battery_dead, {uav.__get_id()}   , {clock_time}, {uav.position}. {uav.state}")
        r.lpush(f"log_test:info_drone:ID={uav._id}:log_state", f"{clock_time},dead")
        exit(1)
    elif uav.__get_battery__() >= 100:
        uav.battery = 100

# Funzione principale
def main():
    s: socket
    uav: UAV
    battery = 100
    s, uav = init_UAV(NET_ID, ID, START_AREA, battery, HOST, PORT)
    uav_start(uav)

if __name__ == "__main__":
    NET_ID = sys.argv[1]
    ID = int(sys.argv[2])
    MAX_CLOCK_TIME = float(sys.argv[3])
    START_AREA: int = sys.argv[4]
    main()
