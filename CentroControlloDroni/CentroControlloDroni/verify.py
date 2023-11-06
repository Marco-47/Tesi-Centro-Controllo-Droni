''' verifica dei requisiti funzionali e non funzionali del sistema'''

#!/usr/bin/env python3
import redis
import logging
import datetime
import time
import sys

# Definizione dei separatori utilizzati nei messaggi
SEPARATOR = '*_*|*'
SEPARATOR2 = '__*__'

# Dichiarazione di variabili globali
time_survillance_max_clock: float = 1800  # Tempo massimo di sorveglianza
clock_time: float = 0.0  # Tempo del clock virtuale
delta_request_time_max: float = 1000  # Tempo massimo attesa
avg_responce_time: float = 0  # Tempo medio di risposta
total_responce_time: float = 0  # Tempo totale di risposta
num_msg: int = 0  # Numero di messaggi elaborati

# Variabili booleane per condizioni di errore
x1: bool = False
x2: bool = False
x3: bool = False
x4: bool = False

# Funzione per verificare i requisiti funzionali e non funzionali del sistema
def verify(r: redis.Redis, net_id: str)->None: 
    global clock_time, time_survillance_max_clock, delta_request_time_max, x1, x2, avg_responce_time, num_msg, total_responce_time, x3, x4

    while True:
        # Estrazione del tempo coda Redis
        data: bytes = r.brpop("queue:verify")[1].decode(encoding='UTF-8')
        print('unlocked')

        if data == 'kill':
            r.lpush("test", 'end_test')
            print("killed verify")
            # Calcolo del tempo medio di risposta
            avg_responce_time = round(total_responce_time / num_msg, 2)
            time.sleep(2)
            # Registrazione di informazioni sull'errore (se presenti)
            logging.error(f"{avg_responce_time}--{total_responce_time}--{num_msg}")
            return (x1, x2)

        clock_time = float(data)

        # Estrazione delle aree di sorveglianza
        area_set: list = r.smembers(f"uav_network:{net_id}:surveillance_areas")
        areas_info: dict = {}
        p: redis = r.pipeline()
        area_decoded = []
        for area in area_set:
            area = area.decode(encoding='UTF-8')
            area_decoded.append(area)
            p.hget(f"uav_network:{net_id}:area:{area}","state")
            p.hget(f"uav_network:{net_id}:area:{area}","time")
        results: list = p.execute()

        for i, area in enumerate(area_decoded):
            state_area: int = int(results[i * 2].decode(encoding='UTF-8'))
            if state_area == 0:
                time_area: float = float(results[i * 2 + 1].decode(encoding='UTF-8'))
                delta_t: float = clock_time - time_area
                if delta_t > time_survillance_max_clock:
                    if not x1:
                        x1 = True
                        # Registrazione di un errore di sorveglianza
                        print("ERROR time survillance", area, delta_t)
                        r.lpush('queue:error', f"time_survillance{SEPARATOR}{clock_time}{SEPARATOR}{area}{SEPARATOR}{time_area}")
                        r.lpush(f"log_test:error", f"{clock_time},survelling_error")

        # Estrazione delle informazioni sui droni 
        drone_set: set = r.smembers(f"uav_network:{net_id}:register_drones")
        drone_info: dict = {}
        p: redis = r.pipeline()
        drone_decoded = []
        for drone in drone_set:
            drone = drone.decode(encoding='UTF-8')
            drone_decoded.append(drone)
            p.hget(f"uav_network:{net_id}:drones:{drone}:state","state")
            p.hget(f"uav_network:{net_id}:drones:{drone}:assigned_area","assigned_area")
        results: list = p.execute()
        print(results, drone_decoded)

        msg = r.rpop('queue:verify-battery')
        while msg:
            time_request: str
            uav_id: str
            status_uav : str
            area_uav: str
            uav_id, time_request, status_uav, area_uav = msg.decode("UTF-8").split(f"{SEPARATOR}")
            if not x4:
                x4 = True
                # Registrazione di un errore della batteria del drone
                r.lpush('queue:error',f"battery{SEPARATOR}{clock_time}{SEPARATOR}{uav_id}{SEPARATOR}{status_uav}")
                r.lpush(f"log_test:error", f"{clock_time},responce_time")
            msg = r.rpop('queue:verify-battery')

        # Sblocco berriera per sincronizzazione microserivizi
        r.lpush("barrier:verify","unlock")

if __name__ == "__main":
    r: redis = redis.Redis(host='localhost', port=6379, db=0)
   
    net_id: str = str(sys.argv[1])
    verify(r, net_id)