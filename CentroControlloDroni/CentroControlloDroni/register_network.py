''' Microservizio utilizzato per la registrazione della mappa dei droni e relative informazioni '''

import redis
from ast import literal_eval
import logging
import datetime
import heapq

# Definizione dei separatori
SEPARATOR = '/sep1*'    
SEPARATOR2 = '/sep2*' 

format_time = "%Y-%m-%d %H:%M:%S.%f"

# Funzione per registrare una rete di droni
def register_network(r: redis, ID: str)-> None:
   
    print(f"{ID} ready!")
    
    while True:
         # Estrazione di un messaggio dalla coda Redis e caching del messaggio in modo da recuperarlo in caso il microservizo muoia durante l'esecuzione
        data: bytes = r.brpoplpush("queue:register_network", f"{ID}_work")
        net_id: str
        map_info: str
        charge_point: str
        net_id, map_info, charge_point = data.decode(encoding='UTF-8').split(f"{SEPARATOR}")
        
        try:
            if r.sismember("net_list", net_id):
                raise Exception(f"La rete con il nome {net_id} esiste già!")
        except Exception as e:
            logging.error(f"{e}")
            msg: str = f"ERROR: {e}"
            p: redis = r.pipeline()
            p.multi()
            p.rpop(f"{ID}_work")
            p.lpush("queue:send_msg_request", msg)
            p.execute()
        else:
            map_dict: dict = literal_eval(map_info)
            surveillance_area_ls: list = set(map_dict.keys())
            start_time: str = 0.0 
            charge_point: set = literal_eval(charge_point)
            p = r.pipeline()
            p.watch(f"uav_network:{net_id}")
            p.multi()
            p.sadd("net_list", net_id)
            p.hset(f"uav_network:{net_id}", "map", map_info)
            p.sadd(f"uav_network:{net_id}:areas_under_surveillance", "")
            for area in surveillance_area_ls:
                area = int(area)
                distances, predecessors = dijkstra(map_dict, area)
                if area in charge_point:
                    p.sadd(f"uav_network:{net_id}:charge_points", f"{area}")
                else:
                    p.sadd(f"uav_network:{net_id}:surveillance_areas", f"{area}")
                p.hset(f"uav_network:{net_id}:area:{area}", "time", f"{start_time}")
                p.hset(f"uav_network:{net_id}:area:{area}", "state", 0)
                p.hset(f"uav_network:{net_id}:area:{area}", "distances", str(distances))
                p.hset(f"uav_network:{net_id}:area:{area}", "predecessors", str(predecessors))
            p.rpop(f"{ID}_work")
            try:
                p.execute()
            except redis.exceptions.WatchError:
                print("La chiave è stata modificata durante la transazione")
                r.brpoplpush(f"{ID}_work", "queue:register_uav")

# Funzione per calcolare il percorso più breve utilizzando l'algoritmo di Dijkstra
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

if __name__ == "__main__":
    r: redis = redis.Redis(host='localhost', port=6379, db=0)
    id: int = int(r.hincrby("register_network", "id", 1))
    ID: str = f"register_network_{id}"
    r.client_setname(f"{ID}")
    r.sadd("microservice_set", ID)
    
    register_network(r, ID)
    
    
