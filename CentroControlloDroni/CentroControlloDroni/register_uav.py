''' Microservizio che svolge il compito di registrare un nuovo drone '''

# Import delle librerie necessarie
"#!/usr/bin/env python3"
import redis
import logging
import datetime
import sys

# Definizione dei separatori
SEPARATOR = '/sep1*'
SEPARATOR2 = '/sep2*'

# Variabili globali per il tempo
clock_time: float = 0.0
work_time: float = 1.0

# Funzione per registrare un nuovo drone
def register_uav(r: redis.Redis, ID: str)->None: 
    global clock_time, work_time

    print(f"{ID} ready!")

    while True:
        # Estrazione di un messaggio dalla coda Redis e caching del messaggio in modo da recuperarlo in caso il microservizio muoia durante l'esecuzione
        data: bytes = r.brpoplpush("queue:register_uav", f"{ID}_work")
    
        # Variabili del messaggio
        simulation_state: int | None  # 0 se la simulazione è in pausa, 1 altrimenti. Se uguale a 0, clock_time non viene aggiornato
        fd: int | None  # File descriptor client 
        clock_time_update: float
        clock_time_request: float | None
        net_id: str | None  # ID della rete del client
        uav_id: str | None  # ID del drone client
        uav_position: int | None
        msg_type: str | None  # Tipo di messaggio per conoscere il tipo di servizio richiesto
        
        # Estrazione delle variabili dal messaggio utilizzando il separatore
        fd, clock_time_update, clock_time_request, net_id, uav_id, uav_position, msg_type, simulation_state = data.decode(encoding='UTF-8').split(f'{SEPARATOR}')
        
        if net_id == 'kill':
            print("killed")
            exit(0)
        
        clock_time_update = float(clock_time_update)
        simulation_state = int(simulation_state)
        
        if simulation_state:
            clock_time_update += work_time
        
        # Controllo per assicurarsi che clock_time non venga decrementato
        if clock_time > clock_time_update:
            logging.error(f"clock_time={clock_time} > clock_time_update={clock_time_update} register_uav")
            
        clock_time = clock_time_update
        
        # Controllo se il net_id è diverso da 'None' (cioè se il drone deve essere registrato in una rete specifica)
        if net_id != 'None':
            clock_time_request = float(clock_time_request)
            try:  # Controllo dell'input
                if not r.sismember("net_list", net_id):
                    raise Exception(f"ID della rete {net_id} non esiste!")
                if r.sismember(f"uav_network:{net_id}:register_drones", uav_id):
                    raise Exception(f"Il drone con ID {uav_id} è già registrato nella rete {net_id}")
            except Exception as e:
                logging.error(f"{e}")
                msg: str = f"{fd}{SEPARATOR}ERROR: {e}"
                p = r.pipeline()
                p.multi()
                p.rpop(f"{ID}_work")
                p.lpush("queue:send_msg", msg)
                p.execute()
            else: 
                # Recupero delle informazioni sulla mappa della rete
                map_info: str = r.hget(f"uav_network:{net_id}", "map").decode(encoding='UTF-8')
                data_msg: str = f"{clock_time}{SEPARATOR2}{map_info}"
                msg: str = f"{fd}{SEPARATOR}{data_msg}"
                
                p: redis = r.pipeline()
                p.watch(f"uav_network:{net_id}:{uav_id}")
                p.multi()
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:state", "state", "registred")
                p.hset(f"fd:{fd}", "info", f"{net_id}{SEPARATOR}{uav_id}")
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:area", "area", f"{uav_position}")
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:time_update", "time_update", f"{clock_time_request}")
                p.hset(f"uav_network:{net_id}:drones:{uav_id}:assigned_area", "assigned_area", "-1")
                p.sadd(f"uav_network:{net_id}:register_drones", uav_id)
                p.rpop(f"{ID}_work")
                p.lpush("queue:send_msg", msg)
                try:
                    p.execute()
                    r.lpush("queue:test_reg", "+1")
                except redis.exceptions.WatchError as e:
                    logging.error(e)
                    r.brpoplpush(f"{ID}_work", "queue:register_uav") 

        else: 
            r.rpop(f"{ID}_work")

        # Se la simulazione è attiva, sblocca il clock_barrier
        if simulation_state:
            r.lpush("clock_barrier", "unlock")   
          
    return

if __name__ == "__main__":
    work_time = float(sys.argv[1])
    
    r: redis = redis.Redis(host='localhost', port=6379, db=0)
    id: int = int(r.hincrby("register_uav", "id", 1))
    ID: str = f"register_uav_{id}"
    r.client_setname(f"{ID}")
    r.sadd("microservice_set", ID)
    r.hincrby("register_uav", "count", 1)
    
    register_uav(r, ID)