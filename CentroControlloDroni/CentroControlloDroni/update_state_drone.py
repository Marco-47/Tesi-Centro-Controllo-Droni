''' Microservizio per aggiornare le informazioni dei droni '''

#!/usr/bin/env python3
import redis
import logging
import datetime
import sys

SEPARATOR = '/sep1*'    
SEPARATOR2 = '/sep2*' 
format_time = "%Y-%m-%d %H:%M:%S.%f"

# Dichiarazione di variabili globali
clock_time: float = 0.0  # Tempo del clock virtuale
work_time: float = 1.0  # Tempo di lavoro

# Funzione per aggiornare lo stato dei droni
def update_state_drone(r: redis, ID: str)->None:
    global clock_time, work_time

    while True:
        # Estrazione di un messaggio dalla coda Redis e caching del messaggio in modo da recuperarlo in caso il microservizo muoia durante l'esecuzione
        data: bytes = r.brpoplpush("queue:update_state_drone", f"{ID}_work")
    
        fd: int | None  # File descriptor del client
        clock_time_update: float
        clock_time_request: float | None
        net_id: str | None  # ID della rete del client
        uav_id: str | None  # ID del drone del client
        uav_position: int
        msg_type: str | None  # Tipo di messaggio per conoscere il tipo di servizi richiesti
        fd, clock_time_update, clock_time_request, net_id, uav_id, uav_position, msg_type, new_state = data.decode(encoding='UTF-8').split(f'{SEPARATOR}')

        clock_time_update = float(clock_time_update)

        if clock_time > clock_time_update:
            logging.error(f"clock_time={clock_time} > clock_time_update={clock_time_update} up state")
            
        clock_time = float(clock_time_update) + work_time
        
        if fd == 'kill':
            print("killed")
            exit(0)

        if fd != 'None':
            clock_time_request = float(clock_time_request)
            time1: float = float(clock_time_request)
            time2: float = float(r.hget(f"uav_network:{net_id}:drones:{uav_id}:time_update", "time_update"))
            if time1 > time2:
                p: redis = r.pipeline()
                p.watch(f"uav_network:{net_id}:drones:{uav_id}:time_update", f"uav_network:{net_id}:drones:{uav_id}:state")
                p.multi()
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:time_update", "time_update", f"{clock_time_request}")
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:state", "state", f"{new_state}")
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:area", "area", f"{uav_position}")
                p.rpop(f"{ID}_work")
                try:
                    p.execute()
                    r.lpush("clock_barrier", f"{ID}")
                except redis.exceptions.WatchError as e:
                    logging.error(e)
                    r.brpoplpush(f"{ID}_work", "queue:update_state_drone")
        else: 
            r.rpop(f"{ID}_work")   
            r.lpush("clock_barrier", f"{ID}")

if __name__ == "__main":
    work_time = float(sys.argv[1])
    
    r: redis = redis.Redis(host='localhost', port=6379, db=0)
    id: int = int(r.hincrby("update_state_drone", "id", 1))
    ID: str = f"update_state_drone_{id}"
    r.client_setname(f"{ID}")
    r.sadd("microservice_set", ID)
    r.hincrby("update_state_drone", "count", 1)
    
    update_state_drone(r, ID)