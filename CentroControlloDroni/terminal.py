'''terminal per lanciare il sistema, i droni e far partire la simulazione'''

import redis
import time
from threading import Thread
import json
import logging
from ast import literal_eval
import socket
import subprocess
import multiprocessing
import os
import signal
import random
import copy
import datetime
import heapq
import shutil

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 9000  # The port used by the server

SEPARATOR = '*_*|*'
SEPARATOR2 = '__*__'

# Store references to the subprocesses globally
processes = []
print_flag: int = 0

def send_command()-> None:
    r: redis.Redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    #s: socket = create_socket_and_connect(HOST,PORT)
    
    while (True):
        command: str = input()
        if "register_network" in command:
            data_ls: list = command.split(' ')
            print(data_ls)
            if len(data_ls) == 2:
                try:
                    f = open(f"./MAPS/{data_ls[1]}.json")
                    jsonF = json.load(f)
                except Exception as e:
                    logging.error(e)
                else:
                    register_network(r,str(jsonF),data_ls[1],"{6,1}")
            else:
                print(f"register_network takes 3 arguments but given {len(data_ls)-1}")
        elif "show_map" in command:
            data_ls: list = command.split(' ')
            if len(data_ls) == 3:
                t: Thread = Thread(target=send_command, args=(data_ls[1],data_ls[2]))
                t.start()
            else:
                 print(f"show_map takes 2 arguments but given {len(data_ls)-1}")
        elif "sim_on" in command:
            send_msg('s','sim_on')
        elif "sim_off" in command:
            send_msg('s','sim_off')
        
        elif "flush_db" in command:
            r.flushdb()
        elif "start_MS" in command:
            data_ls: list = command.split(' ')
            if len(data_ls) == 3:
                launch_MS(data_ls[1],data_ls[2])
            else:
                print(f"start_MS takes 2 arguments but given {len(data_ls)-1}")
        elif "launch_drone" in command:
            data_ls: list = command.split(' ')
            if len(data_ls) == 4:
                launch_drone(data_ls[1], data_ls[2], data_ls[3])
            else:
                print(f"launch_drone takes 3 arguments but given {len(data_ls)-1}")
        elif 'start_test' in command:
            data_ls: list = command.split(' ')
            if len(data_ls) == 6:
                print(data_ls)
                tester(r,data_ls[1],data_ls[2],data_ls[3],data_ls[4], data_ls[5])
            else:
                print(f"start test takes 5 arguments but given {len(data_ls)-1}")
        else:
            print("command not found!")

def register_network(r: redis, map_dict: dict, net_id: str, charge_point: str):
    #print(map_info,net_id,charge_point, type(map_info), type(net_id), type(charge_point))
    num_areas: int = len(map_dict)
    surveillance_area_ls: list = set(map_dict.keys())
    start_time: str =  0.0 #datetime.datetime.now().strftime(f"{format_time}")
    charge_point : set = literal_eval(charge_point) #check input errato ***
    p = r.pipeline()
    p.watch(f"uav_network:{net_id}")
    p.multi()
    p.sadd("net_list", net_id)   #*** gestire eliminazione network + dati + droni registrati
    p.hset(f"uav_network:{net_id}","map",str(map_dict))
    p.sadd(f"uav_network:{net_id}:areas_under_surveillance","") #insieme inizialmente vuoto
    for area in surveillance_area_ls:
        area = int(area)
        distances, predecessors = dijkstra(map_dict, area)
        if area in charge_point:
            p.sadd(f"uav_network:{net_id}:charge_points",f"{area}")
        else:
            p.sadd(f"uav_network:{net_id}:surveillance_areas",f"{area}")
        p.hset(f"uav_network:{net_id}:area:{area}","time",f"{start_time}")
        p.hset(f"uav_network:{net_id}:area:{area}","state",0)
        p.hset(f"uav_network:{net_id}:area:{area}","distances",str(distances))
        p.hset(f"uav_network:{net_id}:area:{area}","predecessors", str(predecessors)) 
    try:
        p.execute()
    except redis.exceptions.WatchError:
        print("La chiave è stata modificata durante la transazione")
      
    return num_areas

def dijkstra(grafo, nodo_iniziale):
    grafo = {int(k): v for k, v in grafo.items()}
    distanze = {nodo: float('inf') for nodo in grafo}
    
    distanze[nodo_iniziale] = 0
    predecessori = {nodo: None for nodo in grafo}
    
    nodi_non_visitati = set(grafo.keys())
    
    while nodi_non_visitati:
        nodo_corrente = min(nodi_non_visitati, key=lambda x: distanze[x])
        nodi_non_visitati.remove(nodo_corrente)
        for vicino, peso in grafo[nodo_corrente]:
            distanza_provvisoria = distanze[nodo_corrente] + peso
            if distanza_provvisoria < distanze[vicino]:
                distanze[vicino] = distanza_provvisoria
                predecessori[vicino] = nodo_corrente
    
    return distanze, predecessori



def create_socket_and_connect(host: str, port: int) -> socket: 
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host,port))
    except socket.error as e:
        logging.error(f"error: {e} | function: create_socket_and_connect()")
        exit(1)
    return client_socket


def send_msg( s: socket, msg: str):
    msg+= '\0'
    try:
        print("msg inviato: " ,msg)
        s.send(bytes(msg, encoding="utf-8"))
    except socket.error as e:
        logging.error(f"error: {e} | function: send_msg()")
        return '-1'
    return 0
  
def tester(r: redis.Redis,  MS_num_istances: str,trasmission_time: float, work_time: float, net_id: str, MAX_CLOCK: str):
    try:
        f = open(f"./MAPS/{net_id}.json")
        jsonF = json.load(f)
    except Exception as e:
            logging.error(e)
            exit(1)
    else:
        map_info: dict = literal_eval(str(jsonF))
        num_area = len(map_info)
        charge_point: int = num_area
        
    print(num_area, charge_point)
    work_time: float = 2
 
    num_drones: int = 1
    trasmission_time = 1.0
    
    
    print("start test drone")
    MAX_CLOCK = 86400
    charge_point = 30
    for i in range(27, 0, -1):
        flag_test = 1
        for num_test in range(0,1):
            launch_test(r, map_info, MS_num_istances,trasmission_time, work_time, net_id, MAX_CLOCK, num_area, i, charge_point)
            if r.llen('queue:error') > 0:
                print(f"Test {net_id} FAIL | areas = {num_area} | charge_point = {charge_point} | drones = {i} | Microservicis = {MS_num_istances} | MAX CLOCK = {MAX_CLOCK}")
                print(r.rpop('queue:error').decode("UTF-8").split(f"{SEPARATOR}"))
                num_drones = i + 1
                flag_test = 0
                break
                #exit(0)
                
        if flag_test:
            print(f"Test {net_id} OK | areas = {num_area} | charge_point = {charge_point} | drones = {i} | Microservicis = {MS_num_istances} | MAX CLOCK = {MAX_CLOCK}")
            exit(0)
        else:
            break
   
    exit()
    
    return

def launch_test(r: redis.Redis, map: dict,  MS_num_istances: str,trasmission_time: float, work_time: float, net_id: str, MAX_CLOCK: str, num_area: int, num_drones: int, charge_point: int):
    map_info: dict = copy.deepcopy(map)
    r.flushdb()
    set_charge_point: set = add_charge_point(map_info, num_area, charge_point)
    print(f'set {set_charge_point}')
    register_network(r,map_info,net_id,str(set_charge_point))
    launch_api(trasmission_time,work_time, MS_num_istances)
    r.brpop('api_started') #barrier: wait until API is not started
    s: socket = create_socket_and_connect(HOST,PORT)
    launch_MS(MS_num_istances,work_time)
    launch_drone(net_id, num_drones, MAX_CLOCK, num_area)
    launch_verify(net_id)
    wait_all_drones(r, num_drones)  
    send_msg(s,'sim_on')
    first_time = datetime.datetime.now()
    print("block")
    r.brpop('test') #barrier: wait until test is not finished
    print((datetime.datetime.now()-first_time).total_seconds())
    s.close()
    kill_spawned_processes()

def wait_all_drones(r: redis.Redis, num_drones: int):
    print("wait_all_drones_register")
    drone_registered: int = 0
    
    while drone_registered < num_drones:
        r.blpop("queue:test_reg")
        drone_registered+=1
    
    return

def launch_api(trasmission_time: str, work_time: str, MS_istance: str):
    
    scripts_folder = os.path.join(os.path.dirname(__file__), 'CentroControlloDroni')   
    script_path = os.path.join(scripts_folder, 'api_comm.py')
    if print_flag:
        p = subprocess.Popen(['python3', script_path, str(work_time),str(trasmission_time), MS_istance], start_new_session=True)
    else:
        p = subprocess.Popen(['python3', script_path, str(work_time),str(trasmission_time), MS_istance], start_new_session=True)
        #p = subprocess.Popen(['python3', script_path, str(work_time), MS_istance],stdout=subprocess.DEVNULL, start_new_session=True)
    processes.append(p)
    
    return

def launch_MS(MS_num_istances: str, work_time: float):
    # Definiamo la lista degli script da eseguire
    scripts = ['register_uav.py', 'set_work.py', 'update_state_drone.py', 'update_area.py', 'unregister_uav.py']

    # Definiamo il float da passare come input
    

    # Otteniamo il percorso assoluto alla cartella "CentroControlloDroni"
    scripts_folder = os.path.join(os.path.dirname(__file__), 'CentroControlloDroni')

    # Avviamo tutti gli script contemporaneamente come demoni
    i = 0
    MS_num_istances = MS_num_istances.split('-')
    
    for script in scripts: 
        script_path = os.path.join(scripts_folder, script)
        # Capture stdout and stderr with subprocess.PIPE
        print(MS_num_istances[i],i)
        for _ in range(int(MS_num_istances[i])):
            print(f'avvio {script}')
            if print_flag:
                p = subprocess.Popen(['python3', script_path, str(work_time)], start_new_session=True)
            else:
                p = subprocess.Popen(['python3', script_path, str(work_time)], stdout=subprocess.DEVNULL, start_new_session=True)
            
            
            processes.append(p)  # Store reference to the subprocess
            
        i+=1
        
    print('microservizi lanciati')
    
def launch_drone(net_id: str, num_UAV: int, max_clock: float, num_area: int):
    scripts_folder = os.path.join(os.path.dirname(__file__), 'UAV')
    num_UAV = int(num_UAV)
    global processes  # Use global variable to store references to the subprocesses
    # Avviamo tutti gli script contemporaneamente come demoni
    for i in range(num_UAV):
        script_path = os.path.join(scripts_folder, 'UAV.py')
        start_area: int = random.randint(0,num_area-1)
        #start_area: int = i
        if print_flag:
            p = subprocess.Popen(['python3', script_path, str(net_id), str(i), str(max_clock), str(start_area)], start_new_session=True)
        else:
            p = subprocess.Popen(['python3', script_path, str(net_id), str(i), str(max_clock), str(start_area)],stdout=subprocess.DEVNULL, start_new_session=True)
        
        processes.append(p)  # Store reference to the subprocess

    print('droni lanciati')

def launch_verify(net_id: str):
    scripts_folder = os.path.join(os.path.dirname(__file__), 'CentroControlloDroni')
    script_path = os.path.join(scripts_folder, 'verify.py')
    if print_flag:
        p = subprocess.Popen(['python3', script_path, net_id], start_new_session=True)
    else:
        #p = subprocess.Popen(['python3', script_path, net_id],stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, start_new_session=True)
        
        p = subprocess.Popen(['python3', script_path, net_id],stdout=subprocess.DEVNULL, start_new_session=True)

    processes.append(p)
    
    print('verify launched!')
    return

def add_charge_point(map_info: dict, num_area: int, num_charge_point: int):
    ls_node = list(range(num_area))
    added_area: set = set()
    
    for i in range(num_area,num_area+ num_charge_point):
        area_index = random.randint(0, len(ls_node)-1)
        new_area = ls_node[area_index]
        distance_to_node = random.randint(30,75)
        map_info[str(new_area)].append([i, distance_to_node])
        map_info[str(i)] = [[new_area, distance_to_node]]
        del ls_node[area_index]
        added_area.add(i)
    
    return str(added_area)

def handle_error_MS_test(r: redis.Redis,  MS_num_istances: str):
    MS_num_istances = MS_num_istances.split('-')
    new_MS_istance: str = MS_num_istances[:]
    
    msg = r.rpop('queue:error')
    print(msg)
    exit(1)
    while msg:
        #print(msg)
        msg = msg.decode("UTF-8").split(f"{SEPARATOR}")
        if ('set_work' in msg) or ('time_survillance' in msg) or ('battery' in msg):
            if new_MS_istance[1] == MS_num_istances[1]:
                new_MS_istance[1] = str(int(new_MS_istance[1])+1)
                print('new_set_work', msg)
        elif 'update_state_drone' in msg:
            if new_MS_istance[2] == MS_num_istances[2]:
                new_MS_istance[2] = str(int(new_MS_istance[2])+1)
                print('update_state_drone')
        elif 'update_area' in msg:
            if new_MS_istance[3] == MS_num_istances[3]:
                new_MS_istance[3] = str(int(new_MS_istance[3])+1)
                print('update_state_drone')
        else:
            logging.error(msg)
        msg = r.rpop('queue:error')
        
    
    return '-'.join(new_MS_istance)

# Set up signal handler to terminate all processes on SIGINT
def sigint_handler(signum, frame):
    print('Received SIGINT, terminating processes...')
    kill_spawned_processes()
    exit(0)
    
def kill_spawned_processes():
    global processes  # Use global variable to access references to the subprocesses
    
    for p in processes:
        p.terminate()  # Send SIGTERM signal to each process

def save_test_result(r: redis.Redis, net_id: str,num_area: int, num_drone: int, num_charge: int, num_istance: str, work_time: float, clock_max: float):
    test_name = f'{num_drone}-{num_charge}-[{num_istance}]-{work_time}-{clock_max}'
    directory = f"./results/{net_id}/{test_name}"
    
    if os.path.exists(directory):
        shutil.rmtree(directory)

    os.makedirs(directory)

    #save drone results
    
    for i in range(num_drone):
        drone_dir = f"{directory}/drone/{i}"
        os.makedirs(drone_dir)
        filename = "log_state.txt"
        queue: str = f"log_test:info_drone:ID={i}:log_state"
        save_info_to_file(r, drone_dir, filename, queue)
        filename = "log_battery.txt"
        queue: str = f"log_test:info_drone:ID={i}:battery_log"
        save_info_to_file(r, drone_dir, filename, queue)
        
    for i in range(num_area):
        area_dir = f"{directory}/area/{i}"
        os.makedirs(area_dir)
        filename = "log_state.txt"
        queue: str = f"log_test:area:{i}"
        save_info_to_file(r, area_dir, filename, queue)
    
def save_info_to_file(r: redis.Redis, dir: str, filename: str, redis_queeue_name: str):
    log_state = ''
    len_log_state = r.llen(redis_queeue_name)
    for _ in range(len_log_state):
        log_msg = r.rpop(redis_queeue_name).decode(encoding='UTF-8')
        log_state+=f'{log_msg}\n'
    log_state = log_state[:-1]
    filepath = os.path.join(dir, filename)
    with open(filepath, "w") as f:
        f.write(log_state)
    
# Register signal handler
signal.signal(signal.SIGINT, sigint_handler)

if __name__ == "__main__":
    send_command()
    #save_test_result('aa','rete5',10,15,'1-2-2-1-1')



