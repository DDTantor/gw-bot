import discord
import database
import time
import log_parser
from discord.ext import commands
from variables import *
import pandas

TOKEN = DISCORD_TOKEN
log_discard_time = 3.0

def log_insert(log, db):
    with db.cursor() as cursor:
        log_data = log_parser.get_log_data(log)
        log_date, log_dur, log_class, success, boss_name = log_parser.get_log_insert_info(log_data) 
        # check if log already exists in db based on time
        if float(log_dur) < log_discard_time or database.is_duplicate(log_data, cursor):
            return 
        
        log_date, log_dur, log_class, success, boss_name = log_parser.get_log_insert_info(log_data) 
        
        # Enter the log into the log_table and return log id
        log_id = database.insert_log(log, log_date, log_dur, log_class, success, boss_name, cursor)

        # Enter phases into the phase_table and return last_phase_id
        last_phase_id, phase_count, starts, ends = database.insert_phases(log_data, log_id, success, cursor)
        
        # Enter players into the player_table
        database.insert_players(log_data, last_phase_id, phase_count, starts, ends, cursor)
        
if __name__ == '__main__':
    fl = pandas.read_csv(r"log_table.csv")
    db = database.connect();
    cnt = 0;
    total_time = 0;
    for i, log_link in enumerate(fl["Log"]):
        t0 = time.time()
        log_insert(log_link, db)
        t1 = time.time()
        print(log_link)
        print("Log inserted")
        print(t1 - t0)
        total_time += t1 - t0;
        cnt += 1;
        if i % 5 == 0:
            db.commit()

    print("Done")
    print("Number of logs: {}".format(cnt))
    print("Total time taken: {}".format(total_time))
    db.close()