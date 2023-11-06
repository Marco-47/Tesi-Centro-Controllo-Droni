'''Microservizio che ha il compoto di assegnare le differenti zone ai droni. In base allo stato del drone e la posizione sulla mappa viene deciso
se il drone andrà in una stazione di ricarca o in una zona da sorvegliare, la zone da sorvegliare viene decisa in base alla distanza e la priorità 
della zona (che aumenta con l'aumentare del tempo in cui non è stata sorvegliata)'''

import redis
from ast import literal_eval
import logging
from datetime import datetime
import sys
import random


SEPARATOR = '/sep1*'    
SEPARATOR2 = '/sep2*' 

format_time = "%Y-%m-%d %H:%M:%S.%f"

clock_time: float = 0.0
work_time: float = 1.0

def set_work(r: redis.Redis, ID: str)-> None:
    global clock_time, work_time
   
    r: redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    while True:
          # Estrazione di un messaggio dalla coda Redis e caching del messaggio in modo da recuperarlo in caso il microservizo muoia durante l'esecuzione
        data: tuple(bytes,bytes) = r.brpoplpush("queue:set_work",f"{ID}_work")
        
        fd: int | None  #file descriptor client 
        clock_time_update: float
        clock_time_request: float | None
        net_id: str | None  #network id of the client
        uav_id: str | None  #id of client
        uav_position: int
        msg_type: str | None  #msg type to know the type of services requested
        battery: float
        fd, clock_time_update, clock_time_request, net_id, uav_id,uav_position, msg_type, battery = data.decode(encoding='UTF-8').split(f'{SEPARATOR}')
      
        clock_time_update = float(clock_time_update)
      
        if clock_time > clock_time_update:
            #logging.error(f"clock_time={clock_time} > clock_time_update={clock_time_update} set_work")
            clock_time_update = clock_time
            
        clock_time = float(clock_time_update)+ work_time
        
        
        if fd == 'kill':
            print("killed")
            exit(0)
      
        if fd != 'None':
            clock_time_request = float(clock_time_request)
            try:  #controllo dati input validi
                if not r.sismember("net_list",net_id):
                    raise Exception(f"net id {net_id} dosen't exist!") 
                if not r.sismember(f"uav_network:{net_id}:register_drones",uav_id):
                    raise Exception(f"uav whit ID {uav_id} is not register to {net_id}!")
            except Exception as e:
                logging.error(f"{e}")
                msg: str = f"{fd}{SEPARATOR}ERROR: {e}"
                p = r.pipeline()
                p.multi()
                p.rpop(f"{ID}_work")
                p.lpush("queue:send_msg",msg)
                p.execute() 
            
            
            battery = float(battery) 
            
            
            next_area: int = -1
            area_type: int = -1
            results: list
            area_ls: set
            
            p: redis = r.pipeline()
            
            if battery < 40:  #se la carica è minore di 40 il drone viene inviato in una zona di ricarica
                charge_points_ls: list = r.smembers(f"uav_network:{net_id}:charge_points") 
                results, area_ls = get_info_area(p,charge_points_ls,net_id,0)
                distance_info_areas = literal_eval(r.hget(f"uav_network:{net_id}:area:{uav_position}","distances").decode(encoding="UTF-8")) 
                distance_to_area: float = float('inf')
                for i, area in enumerate(area_ls):
                    state_area = int(results[i].decode())
                    area = int(area)
                    dist: int = distance_info_areas[area]
                    if state_area == 0 and dist < distance_to_area:
                        distance_to_area = dist
                        next_area = area 
                  
            
            if next_area == -1:  #se il drone non è stato assegnato ad un area di ricarica asseggna una zona da sorvegliare
                area_set : list = r.smembers(f"uav_network:{net_id}:surveillance_areas")
                results, area_ls = get_info_area(r,area_set,net_id,1)
                areas_info : dict = {}
                distance_info_areas = literal_eval(r.hget(f"uav_network:{net_id}:area:{uav_position}","distances").decode(encoding="UTF-8")) 
                
                
                area_min: int = None
                next_area = -1
                
                for i, area in enumerate(area_ls):  #ciclo for per assegnazione area da sorvegliare
                    state_area = int(results[i * 2].decode())
                    if state_area == 0:
                        areas_info[area] = float(results[i * 2 + 1].decode())
                        distance_area: int = distance_info_areas[int(area)]
                        distance_area = float(results[i * 2 + 1].decode())  + (distance_area/10)
                        if area_min == None or distance_area < area_min:
                            area_min = distance_area
                            next_area = area
                        
                if area_min == None or next_area == -1:
                    logging.error('NONE')        
                
                area_type = 1
                
            else:
                area_type = 0
                
            try:
                predecessor_list =  literal_eval(r.hget(f"uav_network:{net_id}:area:{uav_position}","predecessors").decode(encoding="UTF-8")) 
            except:
                logging.error('EXIT')
                exit()
            path : list = ricostruisci_percorso(predecessor_list, int(next_area))
            
         
                
            msg = f"{fd}{SEPARATOR}{clock_time}{SEPARATOR2}{area_type}{SEPARATOR2}{path}"
            
            p.watch(f"uav_network:{net_id}:area:{next_area}")
            p.multi()
            p.hset(f"uav_network:{net_id}:area:{next_area}", "state", 1)
            p.hset(f"uav_network:{net_id}:drones:{uav_id}:assigned_area","assigned_area",f"{next_area}" )
            if area_type == 1:
                p.sadd(f"uav_network:{net_id}:areas_under_surveillance",f"{next_area}")
            p.rpop(f"{ID}_work")
            p.lpush("queue:send_msg",msg)
            p.lpush(f"log_test:area:{next_area}", f"{clock_time_request},assigned")
            try:
                p.execute()
                r.lpush("clock_barrier",f"{ID}") 
            except redis.exceptions.WatchError as e:
                logging.error(f"{e}")
                r.brpoplpush(f"{ID}_work", "queue:set_work")
        
        else: 
            r.rpop(f"{ID}_work")
            r.lpush("clock_barrier",f"{ID}")  
         
    return


def get_info_area(r: redis.Redis, area_ls : list, net_id : str, type: int):
    
        p: redis.Redis = r.pipeline()
        areas_info : dict = {}
        area_decoded = []
        p.multi()
        for area in area_ls:
            area = area.decode(encoding='UTF-8')
           
            area_decoded.append(area)
            p.hget(f"uav_network:{net_id}:area:{area}","state")
            if type == 1:
                p.hget(f"uav_network:{net_id}:area:{area}","time")
        results = p.execute()
        return results, area_decoded
        
        
def ricostruisci_percorso(predecessori, destinazione):  #restituisce in sequenza il percorso che dovrà fare il drone per arrivare alla zona assegnata
    
    percorso = [destinazione]
    while predecessori[destinazione] != None:
        destinazione = predecessori[destinazione]
        percorso.append(destinazione)
    return percorso[::-1]


if __name__ == "__main__":
    work_time = float(sys.argv[1])
    
    r: redis = redis.Redis(host='localhost', port=6379, db=0)
    id: int = int(r.hincrby("set_work", "id", 1))
    ID: str = f"set_work_{id}"
    r.client_setname(f"{ID}")
    r.sadd("microservice_set", ID)
    r.hincrby("set_work", "count", 1)
    
    set_work(r, ID)
    