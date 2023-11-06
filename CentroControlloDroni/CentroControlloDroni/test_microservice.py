import redis
import datetime
import json
import logging
import sys
import time


SEPARATOR = '/sep1*'  
SEPARATOR2 = '/sep2*' 



def test_microservice(name: str, num_msg: int, total_time: int, type_rate: str, distribution: list = None):
    r: redis = redis.Redis(host='localhost', port=6379, db=0, client_name="controller")
    
    sleep_time: float = total_time/num_msg
    num_fin_msg: int = 0
    
    net_id = "rete1"
    i = 0
    
    while num_fin_msg < num_msg:
        
        msg: str = f"{datetime.datetime.now().strftime('%Y%m%d%H:%M:%S.%f')}{SEPARATOR}register{SEPARATOR}{net_id}{SEPARATOR}{i}{SEPARATOR}{0}"
        r.lpush(f"queue:{name}",msg)
        
        i+=1
        num_fin_msg+=1
        #time.sleep(sleep_time)
    
    print("finito")
    
    
def main(microservice:str, jsonPathInfoRateMsg: str):
    try:
        fInfoRate = open(jsonPathInfoRateMsg)
        jsonInfoRate = json.load(fInfoRate)
    except Exception as e:
        logging.error(e)
        sys.exit(1)
    try:
        num_msg: int = jsonInfoRate["num_msg"]
        total_time: int = jsonInfoRate["time_in_sec"]
        type_rate: str = jsonInfoRate["type_rate"]
    except Exception as e:
        logging.error(e)
        sys.exit(1)
    
    test_microservice(microservice, num_msg, total_time, type_rate)
    
                              
    
   
if __name__ == "__main__":     
    if len(sys.argv) != 3:
            print("Usage: python program.py <arg1> <arg2>")
            sys.exit(1)
            
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    print("First argument:", arg1)
    print("Second argument:", arg2)
    
    main(arg1,arg2)