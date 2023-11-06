#!/bin/bash

# Nomi degli script Python da eseguire
script1="unregister_uav.py 1.0"
script2="register_uav.py 1.0"
script3="set_work.py 1.0"
script4="update_state_drone.py 1.0"
script5="update_area.py 1.0"
script6="unregister_uav.py 1.0"

# Variabile per memorizzare i PID dei processi Python
pids=()

# Funzione per eseguire uno script Python e leggere l'output in tempo reale
run_python_script() {
  script="$1"
  echo "Avvio di $script"
  python3 -u $script 2>&1 | while read line; do echo "$script output: $line"; done &
  pids+=($!)
}

# Esegui gli script Python utilizzando la funzione run_python_script
run_python_script $script1
run_python_script $script2
run_python_script $script3
run_python_script $script4
run_python_script $script5
#run_python_script $script6

# Loop per controllare l'input dell'utente
while true; do
  read -n1 -p "Premi q e INVIO per terminare gli script " input
  if [[ $input == "q" ]]; then
    break
  fi
done

# Termina tutti i processi Python avviati dallo script
for pid in $(pgrep -P $$); do
  if [[ $(ps -o comm= $pid) == "python3" ]]; then
    kill $pid
  fi
done

