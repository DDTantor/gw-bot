import requests as rq
import json
import re
import datetime
import database

def get_log_data(log, writeinfile = False):
    # Returns the json of logData from the log as a dictionary 
    # Need to request the main log page because the api returns less info
    #
    request_http_full = rq.get(log)
    source = request_http_full.text
    
    if writeinfile:
        f = open("sourcetemp.txt", "a+")
        f.truncate(0)
        f.write(source)

    p1 = source.find("logData = ")
    p11 = source.find("{", p1)
    p2 = source.find(";", p1)
    if writeinfile:
        f = open("temp.txt", "a+")
        f.truncate(0)
        f.write(source[p11:p2])
    return json.loads(source[p11:p2])

def get_log_insert_info(log_data):
    # Return log_date, log_dur, log_class, success, boss_name to be able to insert into log_table
    tmp         = list(map(int, re.sub('\D', ' ', log_data['encounterDuration']).split()))
    log_date    = datetime.datetime.strptime(log_data['encounterStart'], '%Y-%m-%d %H:%M:%S %z')
    utc         = datetime.timezone(datetime.timedelta())
    log_date    = log_date.astimezone(utc)
    success     = log_data['success']
    log_dur     = "{:.3f}".format(tmp[0] * 60 + tmp[1] + tmp[2] / 1000);
    log_class   = log_data['players'][0]['condi'] > 0;
    boss_name   = log_data['fightName'][:4] # Delay's solution because Skorvald is bugged
    return log_date, log_dur, log_class, success, boss_name

def get_phase_insert_info(phase, last_phase_failed):
    # Return phase_name, start, end,
    # If the phase did not end sucessfully then make end arbitrarily high so it filters out
    # you still want the phase so you can look at when you enter the phase and dps at start
    phase_name = phase['name']
    start      = phase['start']
    end        = 10000 if last_phase_failed else phase['end'] 
    phase_dur  = end - start
    return phase_name, start, end, phase_dur

def get_player_insert_info(log_data, player_it, start, end):
    # Return player_name, class_name, startDPS, endDPS, phaseDPS
    tmp         = log_data['graphData']['phases'][0]['players'][player_it]['damage']['targets'][0];
    e           = -1 if int(end) > 9999 else int(end) + 1 # adjusted end
    s           = int(start)
    player_name = log_data['players'][player_it]['acc'].lower()
    class_name  = log_data['players'][player_it]['profession'].lower()
    startDPS    = tmp[s] / (start + 0.00001) # Avoid division by 0
    endDPS      = tmp[e] / end
    phaseDPS    = (tmp[e] - tmp[s]) / (end - start)
    return player_name, class_name, startDPS, endDPS, phaseDPS

if __name__ == "__main__":
    #log_data = get_log_data("https://dps.report/DHeq-20210908-174116_arriv")
    log_data = get_log_data("https://dps.report/Gtno-20210830-180153_arkk")
    log_date, log_dur, log_class, success, boss_name = get_log_insert_info(log_data)
    print(str(log_date), log_dur, log_class, success, boss_name)
    success = log_data['success'];
    for phase in log_data['phases']:
        if phase['breakbarPhase']:
            continue

        # Last variable here is to filter out last phases of a boss where it failed
        phase_name, start, end, phase_dur = get_phase_insert_info(phase, not success and phase == log_data['phases'][-1])
        print(phase_name, start, end, phase_dur)
        # print(get_player_insert_info(log_data, 0, start, end))
        for j, player in enumerate(log_data['players']):
        #    # j - num of player
            print(get_player_insert_info(log_data, j, start, end))