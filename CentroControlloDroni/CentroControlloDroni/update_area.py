'''Microservizio utilizzato per aggiornare lo stato dell'area'''

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

# Funzione per aggiornare lo stato dell'area
def update_area(r: redis, ID: str)->None:
    global clock_time, work_time

    while True:
        # Estrazione di un messaggio dalla coda Redis e caching del messaggio in modo da recuperarlo in caso il microservizio muoia durante l'esecuzione
        data: bytes = r.brpoplpush("queue:update_area", f"{ID}_work")

        fd: int | None  # File descriptor client
        clock_time_update: float
        clock_time_request: float | None
        net_id: str | None  # Network ID del client
        uav_id: str | None  # ID del client
        uav_position: int
        msg_type: str | None  # Tipo di messaggio per conoscere il tipo di servizio richiesto
        update_type: str | None
        # Estrazione delle variabili dal messaggio utilizzando il separatore
        fd, clock_time_update, clock_time_request, net_id, uav_id, uav_position, msg_type, update_type = data.decode(encoding='UTF-8').split(f'{SEPARATOR}')

        clock_time_update = float(clock_time_update)

        if clock_time > clock_time_update:
            logging.error(f"clock_time={clock_time} > clock_time_update={clock_time_update} up area")

        clock_time = float(clock_time_update) + work_time

        if fd == 'kill':
            print("killed")
            exit(0)

        if fd != 'None':
            clock_time_request = float(clock_time_request)

            time1: float = float(clock_time_request)
            time2: float = float(r.hget(f"uav_network:{net_id}:area:{uav_position}", "time").decode(encoding="UTF-8"))

            p: redis = r.pipeline()
            if time1 >= time2:
                p.watch(f"uav_network:{net_id}:area:{uav_position}")
                p.multi()
                if update_type == 'task_completed':
                    p.hset(f"uav_network:{net_id}:area:{uav_position}", "state", 0)
                    r.lpush(f"log_test:area:{uav_position}", f"{clock_time_request},free")
                elif update_type == 'start_task':
                    p.hset(f"uav_network:{net_id}:area:{uav_position}", "state", 2)
                    r.lpush(f"log_test:area:{uav_position}", f"{clock_time_request},on surveilling")
                p.hset(f"uav_network:{net_id}:area:{uav_position}", "time", clock_time_request)
                p.rpop(f"{ID}_work")
                try:
                    p.execute()
                    r.lpush("clock_barrier", f"{ID}")
                except redis.exceptions.WatchError as e:
                    logging.error(e)
                    r.brpoplpush(f"{ID}_work", "queue:update_area")

        else:
            r.rpop(f"{ID}_work")
            r.lpush("clock_barrier", f"{ID}")

    return

if __name__ == "__main__":
    work_time = float(sys.argv[1])

    r: redis = redis.Redis(host='localhost', port=6379, db=0)
    id: int = int(r.hincrby("update_area", "id", 1))
    ID: str = f"update_area_{id}"
    r.client_setname(f"{ID}")
    r.sadd("microservice_set", ID)
    r.hincrby("update_area", "count", 1)

    update_area(r, ID)