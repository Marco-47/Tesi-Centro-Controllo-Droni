'''Microservizio utilizzato per eliminare le info di un drone che si è scollegato'''

# Import delle librerie necessarie
#!/usr/bin/env python3
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

# Funzione per eliminare le informazioni di un drone scollegato
def unregister_uav(r: redis, ID: str)->None:
    global clock_time, work_time

    while(True):
        # Estrazione di un messaggio dalla coda Redis e caching del messaggio in modo da recuperarlo in caso il microservizio muoia durante l'esecuzione
        data: bytes = r.brpoplpush("queue:unregister_uav", f"{ID}_work")

        fd: int | None  # File descriptor client
        clock_time_update: float
        clock_time_request: float | None
        net_id: str | None  # Network ID del client
        uav_id: str | None  # ID del client
        uav_position: int
        msg_type: str | None  # Tipo di messaggio per conoscere il tipo di servizio richiesto
        lost_connection_message: float
        # Estrazione delle variabili dal messaggio utilizzando il separatore
        fd, clock_time_update, clock_time_request, net_id, uav_id, uav_position, msg_type, lost_connection_message = data.decode(encoding='UTF-8').split(f'{SEPARATOR}')

        clock_time_update = float(clock_time_update)

        if clock_time > clock_time_update:
            logging.error(f"clock_time={clock_time} > clock_time_update={clock_time_update}   unreg")

        clock_time = float(clock_time_update) + work_time

        if fd == 'kill':
            print("killed")
            exit(0)

        if fd != 'None':
            try:  # Controllo dati di input validi
                if not r.exists(f'fd:{fd}'):
                    raise Exception(f"fd = {fd} doesn't exist!") 
            except Exception as e:
                logging.error(f"{e}")
                msg: str = f"{fd}{SEPARATOR}ERROR: {e}"

            # Ottenere lo stato e l'area del drone
            status_uav = r.hget(f"uav_network:{net_id}:drones:{uav_id}:state", 'state').decode(encoding='UTF-8')
            area_uav = r.hget(f"uav_network:{net_id}:drones:{uav_id}:assigned_area","assigned_area").decode(encoding='UTF-8')

            p: redis = r.pipeline()  
            p.watch(f"uav_network:{net_id}:drones:{uav_id}")
            p.multi()

            if status_uav in ('charging', 'surveilling', 'moving_to_designed_position'): 
                # Se il drone è in carica, in sorveglianza o in movimento verso una posizione designata
                # Imposta lo stato dell'area a "free" e aggiorna il tempo se non è in movimento verso una posizione designata
                p.hset(f"uav_network:{net_id}:area:{area_uav}", "state", 0)
                if status_uav != 'moving_to_designed_position':
                    p.hset(f"uav_network:{net_id}:area:{area_uav}", "time", clock_time_request)
                r.lpush(f"log_test:area:{uav_position}", f"{clock_time_request},free")

            # Rimuovi il drone dall'elenco dei droni registrati e cancella le informazioni relative al drone
            p.srem(f"uav_network:{net_id}:register_drones", f"{uav_id}")
            p.delete(f'fd:{fd}')
            p.delete(f"uav_network:{net_id}:drones:{uav_id}:area")
            p.delete(f"uav_network:{net_id}:drones:{uav_id}:state")
            p.delete(f"uav_network:{net_id}:drones:{uav_id}:time_update")

            p.rpop(f"{ID}_work")
            try:
                p.execute()
                r.lpush("queue:verify-battery", f"{uav_id}{SEPARATOR}{clock_time}{SEPARATOR}{status_uav}{SEPARATOR}{area_uav}")
                r.lpush("clock_barrier", f"{ID}")
            except redis.exceptions.WatchError as e:
                print(e)
                r.brpoplpush(f"{ID}_work", "queue:unregister_uav") 
            
        else:
            r.rpop(f"{ID}_work")
            r.lpush("clock_barrier", f"{ID}")

    return

if __name__ == "__main__":
    work_time = float(sys.argv[1])

    r: redis = redis.Redis(host='localhost', port=6379, db=0)
    id: int = int(r.hincrby("unregister_uav", "id", 1))
    ID: str = f"unregister_uav_{id}"
    r.client_setname(f"{ID}")
    r.sadd("microservice_set", ID)
    r.hincrby("unregister_uav", "count", 1)

    unregister_uav(r, ID)