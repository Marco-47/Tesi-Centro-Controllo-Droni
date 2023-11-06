'''API che gestisce la comunicazione tra i droni e il sistema che li controlla'''

import socket
import logging
import select
import types
from threading import Thread
import json
import redis
from random import randint
import datetime
import time
import subprocess
import os
import sys

# Indirizzo IP e porta su cui il server ascolta
HOST = "127.0.0.1"
PORT = 9000

# Stringhe speciali usate come Separatori utilizzati nei messaggi. esempio msg:  tipo_richiesta_/sep1*_dato_campo1_/sep2*_dato_campo2....
SEPARATOR = '/sep1*'    #questo separatore delimita la richiesta dai dati del msg. 
SEPARATOR2 = '/sep2*' #questo separotore delimita i vari campi dei dati.

# Funzione per creare un socket del server e metterlo in ascolto
def create_socket_and_listen(host: str, port: int) -> socket: 
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(50)
        server_socket.setblocking(0)
    except socket.error as e:
        logging.error(f"error: {e} | function: create_socket_and_listen()")
        exit(1)
    return server_socket

s: socket

#tempi utilizzati per la simulazione ad eventi discreti
clock_time: float = 0.0  #tempo principale per la simulazione, questo tempo è uguale per tutti i microservizi (per comprendere meglio vedere relazione tesi per sapere come funzione il tempo nella simulazione ad eventi discreti)
sim_status: int = 0
work_time: float = 1.0  #tempo di avanzamento
block = 1

# Funzione per configurare il server multi-client   (funzione decisamente troppo lunga per i miei gusti ma inutile dividerla per gli scopi del progetto)
def setup_server_multi_client()-> None:
    global sim_status, clock_time, work_time
    global block
    server_socket: socket = create_socket_and_listen(HOST, PORT)
    s = server_socket
    epoll: select = select.epoll()
    epoll.register(server_socket.fileno(), select.EPOLLIN)  #utilizzo di epoll per avere gestire in modo efficente un numero elevato di client (l'API è pensata per gestire diversi droni sparsi per diverse parti del mondo)

    fd_socket_map: dict = {}
    client_queue: dict = {}
    client_registered:int = 1
    
    fd_socket_map[server_socket.fileno()] = server_socket
    fd_status: dict = {}

    # Crea un client Redis
    redisClient: redis.Redis = redis.StrictRedis(host='localhost', port=6379, db=0)  #Redis è utilizzato sia come DB che come code per lo scambio di messaggi tra API e MicroServizi 
    
    # Ciclo principale del server
    print("API server partito")
    while True:
        events = epoll.poll(1)
        for fd, event in events:
            if fd is server_socket.fileno():
                c: socket
                addr: str
                c, addr = server_socket.accept()
                c_fd: int = c.fileno()
                print('Got connection from: ', addr)
                c.setblocking(0)
                epoll.register(c)
                fd_socket_map[c_fd] = c
                client_queue[c_fd] = []
            elif event & select.EPOLLIN:
                data: bytes = fd_socket_map[fd].recv(1024)

                if not data:  #disconessione client
                    print(f'Disconnected {addr}')
                    epoll.unregister(fd)
                    del fd_socket_map[fd]
                    if fd in client_queue:
                        del client_queue[fd]
                else:
                    flag_terminal = 0
                    msg_ls: list = data.decode(encoding='UTF-8').split('\0')
                    
                    if msg_ls[0] == "sim_on":   #API è quella che gestisce la simulazione ad eventi discreti, e la simulazione può essere messa in pausa tramite sim_off oppure far ripartire tramite sim_on
                        if fd in client_queue:
                            del client_queue[fd]
                        print("sim_on")
                        sim_status = 1
                    elif msg_ls[0] == "sim_off":
                        if fd in client_queue:
                            del client_queue[fd]
                        sim_status = 0
                        print("sim_off")
                    
                    if sim_status == 1 and len(client_queue) > 0: #sim_status == 1 = sim_on
                        for msg in msg_ls[:-1]:
                            if fd in client_queue:
                                client_queue[fd].append(msg)
                        
                        while len(client_queue) > 0 and all(bool(client_queue[key]) for key in client_queue):  #i msg vengono gestiti se solo se tutti i client (Droni) hanno inviato un msg. --> leggere relazione per capire il perché
                            msg_next = []
                            time_next: float = None
                            
                            for key in client_queue:
                                msg_fields: str = client_queue[key][0]  #si estrai il messaggio visto
                                msg_fields = msg_fields.split(SEPARATOR)
                                time_client = float(msg_fields[0])
                                if time_client < clock_time:
                                    time_client = clock_time
                                    msg_fields[0] = str(time_client) 
                                    new_msg = SEPARATOR.join(msg_fields)
                                    client_queue[key][0] = new_msg
                            
                            for key in client_queue:  #questa ciclo for ricerca il client con il tempo di simulazione inferiore
                                msg: str = client_queue[key][0]  #quando si decodificano i dati passati tramite socket viene restituita una tupla(coppia), il messaggio è in posizione 0
                                time_client = float(msg.split(SEPARATOR)[0])  #tempo di simulazione in cui si trova il client
                                if time_next == None or time_client < time_next: 
                                    time_next = time_client
                                    msg_next.clear()
                                    msg_next.append((key,msg))
                                elif time_next == time_client:
                                    msg_next.append((key,msg))
                                    
                            if clock_time > time_next:
                                logging.error(f"error in clock time! clock_time > time_next: {clock_time} >  {time_next} ")
                            clock_time = time_next
                            MS_quesue_ls = [0, 0, 0, 0, 0]
                            
                            for msg in msg_next:
                                fd_k, msg = msg
                                flag = False
                                if 'register' in msg:  #richiesta di registrazione da parte del drone ad una network di droni
                                    if MS_quesue_ls[0] == 0:  #check se microservizio disponibile oppure già occupato da altro client 
                                        msg = f"{sim_status}{SEPARATOR}{fd_k}{SEPARATOR}{msg}"
                                        redisClient.lpush("queue:register_uav", msg)
                                        msg_split = msg.split(f"{SEPARATOR}")
                                        verify_time_msg(redisClient, msg_split[3], msg_split[7])
                                        client_registered += 1
                                        MS_quesue_ls[0] += 1
                                        client_queue[fd_k].pop(0)
                                    else:
                                        flag = True
                                        
                                if flag:  #caso microservizio già accupato
                                    msg_fields = msg.split(SEPARATOR)
                                    time = float(msg_fields[0]) + work_time
                                    msg_fields[0] = str(time)
                                    new_msg = SEPARATOR.join(msg_fields)
                                    client_queue[fd_k][0] = new_msg
                         
                          
                            for i in range(len(MS_quesue_ls)):  #ciclo for utilizzato come barriera per la sincronizzazione dei microservizi --> vedere tesi per comprendere il motivo
                                if MS_quesue_ls[i] == 0:
                                    MS_queue: str 
                                    if i == 0: MS_queue = "register_uav"
                                    redisClient.brpop("clock_barrier")
                                    redisClient.hset("barrier", "barrier", 0)
                                    clock_time += work_time
                                    redisClient.lpush("queue:verify", f"{clock_time}")
                                    redisClient.brpop("barrier:verify")
                                   
                    else:
                        for msg in msg_ls[:-1]:
                            if 'register' in msg:
                                msg = f"{sim_status}{SEPARATOR}{fd}{SEPARATOR}{msg}"
                                redisClient.lpush("queue:register_uav", msg)
                            else:
                                print("richiesta diversa da rigestrazione mentre simulazione non attiva!!!")
                                if fd in client_queue:
                                    print("new_msg sim_off")
                                    client_queue[fd].append(msg)
    return

# Funzione per inviare messaggi ai client
def sender_msg_UAV()-> None:
    id_socket_map: dict[str : socket] = {}
    
    r: redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    while(True):
        data: tuple(bytes, bytes) = r.brpop("queue:send_msg")
        destination_socket_fd : str
        msg : str
        destination_socket_fd, msg = data[1].decode(encoding='UTF-8').split(f"{SEPARATOR}")
        print(destination_socket_fd)
        if destination_socket_fd == 'kill':
            exit(0)
        try:
            os.write(int(destination_socket_fd), msg.encode(encoding="utf-8"))
        except socket.error as e:
            logging.error(f"error: {e} | function: setup_sender()")
    return

# Funzione per inviare un messaggio "kill" a tutte le code Redis e quindi tutti i microsevizzi
def kill_sistem(r: redis):
    global clock_time
    
    kill_msg: str = f"kill{SEPARATOR}{clock_time}{SEPARATOR}kill{SEPARATOR}kill{SEPARATOR}kill{SEPARATOR}kill{SEPARATOR}kill{SEPARATOR}kill"  #messaggio speciale per kill microservizio
    for MS in ("register_uav", "set_work", "update_state_drone", "update_area", "unregister_uav"):
        r.lpush(f"queue:{MS}",f"{kill_msg}")
    
    kill_msg: str = f"kill{SEPARATOR}kill"
    r.lpush("queue:send_msg",f"{SEPARATOR}")
    r.lpush("queue:verify", "kill") 

# Funzione per verificare il tempo in cui è stato richiesto un messaggio
def verify_time_msg(r: redis.Redis, time_request: str, msg_type: str):  
    r.lpush('queue:verify-time', f"{time_request}{SEPARATOR}{msg_type}")  #log tempi d'attesa per la verifica dei requisiti
# Funzione principale
def main():
    global work_time
    work_time = float(sys.argv[1])
    
    t1: Thread = Thread(target=setup_server_multi_client)
    t2: Thread = Thread(target=sender_msg_UAV)
    
    t1.start()
    t2.start()
    
    return

if __name__ == "__main__":
    main()
