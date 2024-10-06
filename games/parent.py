import json
import subprocess
from datetime import datetime
import re
import os
from fcntl import flock, LOCK_EX, LOCK_UN, LOCK_NB
from queue import Queue
from random import randint
import time
from math import ceil
import argparse

Game_name = ""
step = 100
parser = argparse.ArgumentParser(description=f"Parent Control for game processes")
parser.add_argument('-f','--forced_restart', required=False, nargs ='+', 
                    help='''Forced restart all process, usage:
                    [restart one process: -f process1] | [restart more than one process: -f process1 process2] | [restart all process: -f all]''')
parser.add_argument('-g','--game', required=True, help='Enter game name, usage: [-g Seed]')


def should_exclude_process(process_name):
    excluded_keywords = ["solver-tg-bot", "Telegram-Bot", "http-proxy", "Activating", "Initialising", "name", "daily-update", "All", "Parent"]
    return any(keyword in process_name for keyword in excluded_keywords)

def run_command(command):
    return subprocess.run(command, text=True, shell=True, capture_output=True).stdout

def extract_detail(line, keyword):
    return line.split(f"{keyword}:")[1].strip() if keyword in line else "None"

def fetch_next_calim_from_logs(process_name):
    sanitized_process_name = process_name.replace(':', '-').replace('_', '-')
    log_file = f"/root/.pm2/logs/{sanitized_process_name}-out.log"

    if not os.path.exists(log_file):
        return "None"

    logs = run_command(f"tail -n 200 {log_file}")

    next_claim_at = "None"
    next_claim_at_timestamp = 0
    relevant_lines = [line for line in reversed(logs.splitlines()) if any(kw in line for kw in ["Need to wait until"])]

    for line in relevant_lines:
        if "Need to wait until" in line and next_claim_at == "None":
            next_claim_at = parse_time_from_log(line)
            next_claim_at_timestamp = int(datetime.timestamp(next_claim_at))
    return next_claim_at_timestamp

def parse_time_from_log(line):
    try:
        time_str = line.split("Need to wait until ")[1].split(' before')[0]
        try:
            parsed_time = datetime.strptime(time_str, "%d %B - %H:%M")
            if parsed_time.month > 9:
                parsed_time = parsed_time.replace(year=2024)
            else:
                parsed_time = parsed_time.replace(year=2025)
        except ValueError:
            parsed_time = datetime.strptime(time_str, "%H:%M")
        return parsed_time
    except Exception as e:
        print(f"Failed to parse time from line: {line}. Error: {e}")
        return None

def list_all_pm2_processes():
    lis = run_command("pm2 list --no-color | awk '{{print $4}}'").splitlines()
    lis2 = [i for i in lis if i != ""]
    return list(set(p for p in lis2 if not should_exclude_process(p)))

def list_pm2_processes(status_filter):
    lis = run_command(f"pm2 list --no-color | grep {status_filter} | awk '{{print $4}}'").splitlines()
    return list(set(p for p in lis if not should_exclude_process(p)))

def check_first_setuped():
    status_json_file = "games/utils/status.json"
    if os.path.isfile(status_json_file):
        with open(status_json_file) as f:
            alldata = json.load(f)
        is_setuped = alldata.get("first_setup").get("is_setuped")
        if is_setuped:
            return True
        else:
            return False
    else:
        return False

def first_setup(overrid_check_first_setup = False):
    if not overrid_check_first_setup and check_first_setuped:
        return

    stopped_processes, running_processes, stopping_processes, errored_processes, all_processes = find_processes()

    now_timestamp = int(datetime.timestamp(datetime.now()))
    all_status = {"first_setup": {"is_setuped": False, "first_setup_date": now_timestamp}, "data":{}}

    for name in running_processes:
        next_claim_at = fetch_next_calim_from_logs(name.strip())
        now_timestamp = int(datetime.timestamp(datetime.now()))
        if next_claim_at != "None":
            constructor = {"status": "online", "next_claim_at": next_claim_at, "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": False, "stopped_by_parent": False}
        else:
            constructor = {"status": "None", "next_claim_at": "None", "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": False, "stopped_by_parent": False}
        all_status["data"][name] = constructor

    for name in stopped_processes:
        next_claim_at = fetch_next_calim_from_logs(name.strip())
        now_timestamp = int(datetime.timestamp(datetime.now()))
        if next_claim_at != "None":
            constructor = {"status": "online", "next_claim_at": next_claim_at, "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": True, "stopped_by_parent": False}
        else:
            constructor = {"status": "None", "next_claim_at": "None", "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": True, "stopped_by_parent": False}
        all_status["data"][name] = constructor

    for name in stopping_processes:
        next_claim_at = fetch_next_calim_from_logs(name.strip())
        now_timestamp = int(datetime.timestamp(datetime.now()))
        if next_claim_at != "None":
            constructor = {"status": "stopping", "next_claim_at": next_claim_at, "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": True, "stopped_by_parent": False}
        else:
            constructor = {"status": "None", "next_claim_at": "None", "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": True, "stopped_by_parent": False}
        all_status["data"][name] = constructor

    for name in errored_processes:
        next_claim_at = fetch_next_calim_from_logs(name.strip())
        now_timestamp = int(datetime.timestamp(datetime.now()))
        if next_claim_at != "None":
            constructor = {"status": "errored", "next_claim_at": next_claim_at, "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": False, "stopped_by_parent": False}
        else:
            constructor = {"status": "None", "next_claim_at": "None", "last_claim": 0, "last_check": now_timestamp, "stopped_by_user": False, "stopped_by_parent": False}
        all_status["data"][name] = constructor
        all_status["first_setup"]["is_setuped"] = True

    write_all_data(all_status)

def write_data(name, constructor):
    status_json_file = "games/utils/status.json"
    if not os.path.isfile(status_json_file):
        subprocess.call(f"touch {status_json_file}", shell=True, timeout = 1)
        time.sleep(0.5)
        d = {}
        with open(status_json_file, 'w') as f:
            flock(f, LOCK_EX)
            json.dump(d, f)
            flock(f, LOCK_UN)
    with open(status_json_file) as f:
        alldata = json.load(f)
    alldata[name] = constructor
    with open(status_json_file, 'w') as f:
        flock(f, LOCK_EX)
        json.dump(alldata, f)
        flock(f, LOCK_UN)

def write_all_data(alldata):
    status_json_file = "games/utils/status.json"
    if not os.path.isfile(status_json_file):
        subprocess.call(f"touch {status_json_file}", shell=True, timeout = 1)
        time.sleep(0.5)
        d = {}
        with open(status_json_file, 'w') as f:
            flock(f, LOCK_EX)
            json.dump(d, f)
            flock(f, LOCK_UN)
    # js_object = json.dumps(alldata, indent = 2)
    with open(status_json_file, 'w') as f:
        flock(f, LOCK_EX)
        json.dump(alldata, f, indent = 4)
        flock(f, LOCK_UN)

def check_user_stopped_game():
    stopped_processes, running_processes, stopping_processes, errored_processes, all_processes = find_processes()

    status_json_file = "games/utils/status.json"
    if os.path.isfile(status_json_file):
        with open(status_json_file) as f:
            alldata = json.load(f)

    for name, data in alldata["data"].items():
        stopped_by_user = data["name"]["stopped_by_user"]
        stopped_by_parent = data["name"]["stopped_by_parent"]
        # if name in stopped_processes or stopping_processes and stopped_by_user:
        #     pass

def find_processes():
    stopped_processes = list_pm2_processes("stopped")
    running_processes = list_pm2_processes("online")
    stopping_processes = list_pm2_processes("stopping")
    errored_processes = list_pm2_processes("errored")
    all_processes = list_all_pm2_processes()

    return stopped_processes, running_processes, stopping_processes, errored_processes, all_processes

def check_for_new_game():
    stopped_processes, running_processes, stopping_processes, errored_processes, all_processes = find_processes()

    status_json_file = "games/utils/status.json"
    if os.path.isfile(status_json_file):
        with open(status_json_file) as f:
            alldata = json.load(f)
    else:
        return

    name_list = []
    for name in alldata["data"].keys():
        if name in all_processes:
            name_list.append(name)
        else:
            alldata["data"].pop(name, None)
    # for item in all_process

def check_running_session_by_game(game_name):
    status_file_path = "status.txt"
    with open(status_file_path, "r+") as file:
        status = json.load(file)
        active_sessions = {k: v for k, v in status.items()}
        sessions = []
        if len(active_sessions) > 0:
            for item in active_sessions:
                if game_name in item:
                    session_name = item.split("/")[-1]
                    sessions.append(session_name)
        return sessions

def all_active_sessions():
    status_file_path = "status.txt"
    with open(status_file_path, "r+") as file:
        status = json.load(file)
        active_sessions = {k: v for k, v in status.items()}
        sessions = []
        if len(active_sessions) > 0:
            for item in active_sessions:
                session_name = item.split("/")[-1]
                sessions.append(session_name)
        return sessions

def write_claim_date(process_name):
    pass

def find_next_calim_at_by_game(game_name):
    next_claim_at_dict = {}
    all_processes = list_all_pm2_processes()
    processes = [i for i in all_processes if game_name in i]
    for item in processes:
        nex = fetch_next_calim_from_logs(item)
        next_claim_at_dict[item] = nex
    next_claim_at_dict = dict(sorted(next_claim_at_dict.items(), key=lambda item: item[1]))
    return next_claim_at_dict

def print_sort_next_claim(next_claim_at_dict):
    print("-"*50)
    for name, next_claim_at in next_claim_at_dict.items():
        time.sleep(0.1)
        if next_claim_at < time.time():
            print(f"{name}:\tadded to queue")
        else:
            remain_time = ceil((next_claim_at - time.time())/60)
            print(f"{name}:\tremain time = {remain_time} min")
    print("-"*50)

def clear_log_file(game_name):
    print("Clearing log files ...")
    out = f"../../../root/.pm2/logs/{game_name}-Parent-out.log"
    error = f"../../../root/.pm2/logs/{game_name}-Parent-error.log"
    if os.path.isfile(out):
        run_command(f"echo -> {out}")
        time.sleep(0.1)
    if os.path.isfile(error):
        run_command(f"echo -> {error}")

def stop_all_game_sessions(game_name):
    print(f"Stoping all {Game_name} sessions ..." )
    next_claim_at_dict = {}
    all_processes = list_all_pm2_processes()
    processes = [i for i in all_processes if game_name in i]
    for item in processes:
        cmd = f"pm2 stop {item}"
        run_command(cmd)
        time.sleep(1)

def check_all_game_stopped(game_name):
    stopped_processes = list_pm2_processes("stopped")
    all_processes = list_all_pm2_processes()
    game_processes = [i for i in all_processes if game_name in i]
    running_processes = list_pm2_processes("online")
    sessions = check_running_session_by_game(Game_name)
    for item in game_processes:
        if item in stopped_processes or item in sessions:
            continue
        if item in running_processes:
            run_command(f"pm2 stop {item}")

def print_red(txt: str):
    print("\033[91m{}\033[00m".format(txt))

def print_green(txt: str):
    print("\033[92m{}\033[00m".format(txt))

def print_yellow(txt: str):
    print("\033[93m{}\033[00m" .format(txt))

def print_cyan(txt: str):
    print("\033[96m{}\033[00m" .format(txt))

def print_step():
    global step
    txt = "-" * 50 + f" Step = {step} " + "-" * 50
    print_yellow(txt)

def print_status(next_claim_at_dict):
    global Game_name
    sleep = 0
    error = 0
    running = 0
    queued = 0
    for name, next_claim_at in next_claim_at_dict.items():
        sessions = check_running_session_by_game(Game_name)
        if name in sessions:
            running += 1
            continue
        if next_claim_at == 0:
            error += 1
        elif next_claim_at < time.time():
            queued += 1
        else:
            sleep += 1
    print_cyan(f"STATUS: All = {len(next_claim_at_dict)}  Sleep = {sleep}  Queued = {queued}  Running = {running}  Error = {error}")

def control_game_sessions(forced_restart_processes = []):
    global Game_name, step
    stop_all_game_sessions(Game_name)
    # clear_log_file(Game_name)
    min_time_between_claims = 4 * 60            # 4 min + 1 min sleep = 5 min
    random_time_between_claims = [0, 60]
    max_time_for_claiming_process = 20 * 60     # 20 min = from restart to end claiming process (maybe there is no empty session to use for claiming)
    next_claim_at = {}
    start_claim = time.time()
    end_claim = time.time()
    is_first_time = True
    claim_successful = True
    que = []
    current_process = ""
    if len(forced_restart_processes) > 0:
        forced_restart = True
    else:
        forced_restart = False
    error = 0
    while True:        
        if is_first_time and forced_restart:
            que = forced_restart_processes
            print(f"restart {forced_restart_processes} processes")
        else:
            next_claim_at = find_next_calim_at_by_game(Game_name)
            print_status(next_claim_at)

        if len(forced_restart_processes) == 0:
            forced_restart = False
   
        if claim_successful:
            
            if len(next_claim_at) > 0 and len(forced_restart_processes) == 0:
                for name, next_claim_at_curr in next_claim_at.items():
                    now_timestamp = int(datetime.timestamp(datetime.now()))
                    if next_claim_at_curr < now_timestamp:
                        if name not in que:
                            que.append(name)
            
            if len(que) > 0:
                active_game_sessions = check_running_session_by_game(Game_name)
                if len(active_game_sessions) == 0:                                                                     # don't run two session from one game at the same time
                    time_wait = min_time_between_claims + randint(*random_time_between_claims)
                    if is_first_time or time.time() - end_claim > time_wait:                               # dob't run next session that doesn't reach min sleep time
                        current_process = que.pop(0)
                        now_timestamp = int(datetime.timestamp(datetime.now()))
                        next_claim_at_curr = next_claim_at.get(current_process)
                        if next_claim_at_curr and next_claim_at_curr > now_timestamp:
                            run_command(f"pm2 stop {current_process}")
                            if forced_restart and que != forced_restart_processes:
                                forced_restart_processes.pop(0)
                            continue
                        else:
                            if forced_restart and que != forced_restart_processes:
                                forced_restart_processes.pop(0)
                            step += 1
                            print_step()
                            print_sort_next_claim(next_claim_at)
                            is_first_time = False
                            run_command(f"pm2 restart {current_process}")
                            print(f"{current_process} : restart pm2 process, wait for 60s to process to be completed, {len(que)} process left")
                            claim_successful = False
                            error = 0
                            start_claim = time.time()
                    else:
                        remain_time = ceil((end_claim + time_wait - time.time())/60)
                        print(f"wait for {remain_time} min for next session, minimum wait between two session is 5 min")
                
                else:
                    print(f"we have must wait until {active_game_sessions} process was finished, sleep for 60s")
            
            else:
                print_sort_next_claim(next_claim_at)
                print(f"process list is empty, wait for new claim time reached")

        else:
            next_claim_at_curr = fetch_next_calim_from_logs(current_process.strip())
            
            if next_claim_at_curr == "None":
                print(f"{current_process} : we can't find a valid next claim time in last 200 lines in logs file, maybe log files are flushed or gone, this process will be restarted again after 10 attempts")
                error += 1
                if error > 10:
                    run_command(f"pm2 stop {current_process}")
                    print_red(f"{current_process} : after 10 attempts, we can't find a valid next time claim, this process will be restarted again")
                    que.append(current_process)
                    claim_successful = True
                    check_all_game_stopped()
                    
            
            elif next_claim_at_curr > time.time():
                claim_successful = True
                end_claim = time.time()
                print_green(f"{current_process} : claiming process was successful, it has been stopped in pm2")
                run_command(f"pm2 stop {current_process}")
            
            else:
                claiming_time = ceil(time.time() - start_claim)
                print(f"{current_process} : restart pm2 is under process for {claiming_time}s, let's wait another 60s.")
                print(f"{current_process} : this delay is cuased by one of this 3 problem:")
                print(f"{current_process} : 1. all sessions are occupied. active sessions: {all_active_sessions()}")
                print(f"{current_process} : 2. there is an error in claiming process, check logs with 'pm2 logs {current_process}'.")
                print(f"{current_process} : 3. Just maybe claiming process takes too long time like TimeFarm game.")
                print(f"{current_process} : don't worry, all problems are handling automatically by this code.")

            if time.time() - start_claim > max_time_for_claiming_process:
                run_command(f"pm2 stop {current_process}")
                claim_successful = True
                que.append(current_process)
                print_red(f"{current_process} : we can't find an empty session or maybe this is caused by error, check logs")
                print_green(f"{current_process} : don't worry, if there is no error, this process will be restarted again")
                check_all_game_stopped()


        print("-"*100)
        time.sleep(60)

if __name__ == "__main__":
    args, unknown = parser.parse_known_args()
    forced_restart_list = args.forced_restart
    Game_name = args.game
    if forced_restart_list is None:
        control_game_sessions()
    else:
        valid_process_list = []
        all_processes = list_all_pm2_processes()
        game_processes = [i for i in all_processes if Game_name in i]
        if forced_restart_list[0] == "all":
            control_game_sessions(game_processes)
        else:
            for item in forced_restart_list:
                if item in game_processes:
                    valid_process_list.append(item)
                else:
                    print(f"{item} is not a valid process name")
            control_game_sessions(valid_process_list)
            
