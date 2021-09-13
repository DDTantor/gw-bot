import discord
import database
import time
import log_parser
from discord.ext import commands
from variables import *

TOKEN = DISCORD_TOKEN

def log_insert(log, db):
    try:
        log_data = log_parser.get_log_data(log)
    except Exception as e:
        print("Error when getting log info, exception ", e)    
    try:
        with db.cursor() as cursor:
            log_date, log_dur, log_class, success, boss_name = log_parser.get_log_insert_info(log_data) 
            # Get boss_name_id
            boss_name_id = database.get_name_id(db, "boss_name_table", "bossName", boss_name, cursor)
            
            if float(log_dur) < 3.0 or database.is_duplicate(log_data, cursor):
                return 

            # Enter the log into the log_table
            sql = "INSERT INTO log_table (log, logDate, logDuration, logClass, success, bossNameID) VALUES ('%s', '%s', %s, %s, %s, %s)" \
                   % (log, log_date, log_dur, log_class, success, boss_name_id) 

            cursor.execute(sql)
            log_id = cursor.lastrowid
            
            cnt = 0;
            sql = "INSERT INTO phase_table (logID, phaseNameID, startTime, endTime, phaseDuration) VALUES "
            phase_list = [] 
            starts = []
            ends = []

            # For each phase save the sql entry then insert all at once
            for phase in log_data['phases']:
                # Last variable here is to filter out last phases of a boss where it failed
                if phase['breakbarPhase']:
                    continue

                phase_name, start, end, phase_dur = log_parser.get_phase_insert_info(phase, not success and phase == log_data['phases'][-1])
                phase_name_id = database.get_name_id(db, "phase_name_table", "phaseName", phase_name, cursor)
                phase_list.append("(%s, %s, %s, %s, %s)"  % (log_id, phase_name_id, start, end, phase_dur))
                starts.append(start)
                ends.append(end)


            sql += ",".join(phase_list) + ";"
            cursor.execute(sql)
            phase_id_last = cursor.lastrowid
            player_ids = []
            class_ids = []

            # For each player get the player and class ids
            for j in range(len(log_data['players'])):
                player_ids.append(database.get_name_id(db, "player_name_table", "playerName", log_data['players'][j]['acc'], cursor))
                class_ids.append(database.get_name_id(db, "class_name_table", "className", log_data['players'][j]['profession'], cursor))

            sql = "INSERT INTO dps_table (phaseID, playerNameID, classNameID, startDPS, endDPS, phaseDPS) VALUES "
            dps_list = []

            # For each player save the sql entry then insert all at once
            for phase_id, start, end in zip(range(phase_id_last, phase_id_last + len(phase_list)), starts, ends):    
                #phase_id = get_phase_id(db, phase_name_id, log_id, cursor)
                for j in range(len(log_data['players'])):
                    # j - num of player

                    player_name, class_name, startDPS, endDPS, phaseDPS = log_parser.get_player_insert_info(log_data, j, start, end)
                    player_name_id = player_ids[j]
                    class_name_id  = class_ids[j]
                    dps_list.append("(%s, %s, %s, %s, %s, %s)" % (phase_id, player_name_id, class_name_id, startDPS, endDPS, phaseDPS))


            sql += ",".join(dps_list) + ";"
            cursor.execute(sql)

    except Exception as e:
        print("Error when inserting log, exception ", e)

def log_dur(var, tp, dt, last_patch : bool, db):
    #  $dur "skorv, end, Phase 2" p p
    d = {
        "start" : "startTime", 
        "end" : "endTime", 
        "full" : "phaseDuration"
        }
    t = {
        "p" : "Power",
        "c" : "Condi",
        "a" : "Both"
        }

    with db.cursor() as cursor:
        boss_name_id = database.get_exact_id("boss_name_table", "bossName", var[0], cursor)
        try:
            if len(var) == 1:
                # Only get kills and sort by time
                sql =   "SELECT Log, logDuration " \
                        "FROM log_table " \
                        "WHERE success = 1 AND bossNameID = %s " \
                        % (boss_name_id)
                if tp != 'a':
                    sql += "AND logClass = %s " % (tp == 'c')
                        
                sql +=  "ORDER BY logDuration LIMIT 20" 
                cursor.execute(sql)
                tmp = cursor.fetchall()
                msg = "\n".join([" ".join([x['Log'], t[tp], str(x['logDuration'])]) for x in tmp])
            else:
                # Get phases                
                phase_name_id = database.get_exact_id("phase_name_table", "phaseName", var[2], cursor)
                sql =   "SELECT l.Log, pt.%s " \
                        "FROM log_table AS l " \
                        "INNER JOIN phase_table AS pt ON l.id = pt.logID " \
                        "WHERE pt.phaseNameID = %s AND l.bossNameID = %s AND l.logDate > '%s' " \
                        % (d[var[1]], phase_name_id, boss_name_id, dt)
                if tp != 'a':
                    sql += "AND l.logClass = %s " % (tp == 'c')

                sql += "ORDER BY pt.%s LIMIT 20" % (d[var[1]])

                cursor.execute(sql)
                tmp = cursor.fetchall()
                msg = "\n".join([" ".join([x['Log'], t[tp], var[1], var[2], str(x[d[var[1]]])]) for x in tmp])

        except Exception as e:
            print("Error when fetching log, exception ", e)
            msg = "Your query was bad you monkey, %s" % e

    db.commit()
    return msg

def log_dps(var, tp, dt, last_patch : bool, db):
    # $dps "skor, start, Phase 2, delay/weaver" p p
    d = {
        "start" : "startDPS", 
        "end" : "endDPS", 
        "full" : "phaseDPS"
        }
    t = {
        "p" : "Power",
        "c" : "Condi",
        "a" : "Both"
        }

    with db.cursor() as cursor:

        try:
            try:
                if '/' not in var[3]:
                    play_name_id = database.get_exact_id("player_name_table", "playerName", tmp[0], cursor)
                    class_name_id = ""
                else:
                    tmp = var[3].split('/')
                    player_name_id = "" if tmp[0] == "" else database.get_exact_id("player_name_table", "playerName", tmp[0], cursor)
                    class_name_id = "" if tmp[1] == "" else database.get_exact_id("class_name_table", "className", tmp[1], cursor)
            except:
                player_name_id = ""
                class_name_id = ""

            boss_name_id = database.get_exact_id("boss_name_table", "bossName", var[0], cursor)
            phase_name_id = database.get_exact_id("phase_name_table", "phaseName", var[2], cursor)
            sql =   "SELECT l.Log, d.%s " \
                    "FROM log_table AS l " \
                    "INNER JOIN phase_table AS p ON l.ID = p.logID " \
                    "INNER JOIN dps_table AS d ON p.ID = d.phaseID " \
                    "WHERE l.bossNameID = %s AND p.phaseNameID = %s AND l.logDate > '%s' " \
                    % (d[var[1]], boss_name_id, phase_name_id, dt)  
            
            if player_name_id != "":
                sql += "AND d.playerNameID = %s " % player_name_id

            if class_name_id != "":
                sql += "AND d.classNameID = %s " % class_name_id

            if tp != 'a':
                sql += "AND l.logClass = %s " % (tp == 'c')



            sql += "ORDER BY d.%s DESC LIMIT 20" % (d[var[1]])

            print(sql)
            cursor.execute(sql)
            tmp = cursor.fetchall()
            msg = "\n".join([" ".join([x['Log'], str(x[d[var[1]]])]) for x in tmp])
            
        except Exception as e:
            print("Error when fetching log, exception ", e)
            msg = "Your query was bad you monkey, %s" % e

    return msg

class IV_Bot:
    def __init__(self, token):
        self.bot = commands.Bot(command_prefix = "$")
        self.token = token
        self.prepare_bot()

    def run(self):
        @self.bot.event
        async def on_ready():
            print(self.bot.user.name, "has connected to the server!")

        self.bot.run(self.token)

    def prepare_bot(self):
        # --------------------------------- LOGS --------------------------------- #
        @self.bot.command(name = 'log', help = 'Use to insert logs into the database e.g. $log "link link ..."')
        async def logs(ctx, *args):
            db = database.connect();
            await ctx.message.delete()
            await ctx.send("Inserting logs...")
            for i, log_link in enumerate(args[0].split()):
                t0 = time.time()
                log_insert(log_link, db)
                t1 = time.time()
                print("Log inserted")
                print(t1 - t0)
                if i % 5 == 0:
                    db.commit()

            print("Done")
            await ctx.send("Done inserting logs")
            db.close()

        @self.bot.command(name = 'dur', help = 'Get duration of phases/fights e.g. $dur "skorv, end, Phase 2" -a -p')
        async def dur(ctx, *args):
            # Here we pray people wont write stupid commands
            # ARGUMENTS
            # - boss
            # - type: {start, end, full}
            #          start - when does phase start in log
            #          end   - when does phase end in log 
            #          full  - duration of the phase 
            # - phase
            # 
            # next param is type of log: a - all, p - power, c - condi
            #
            # last parameter in args -p means only check logs after the latest balance patch
            #
            #

            print(args)
            db = database.connect();
            var = [x.strip() for x in args[0].split(',')]
            try:
                tp = args[1]
            except:
                tp = 'a'
            try:
                dt = args[2]
            except:
                dt = ""
            try:
                last_patch = args[3]
            except:
                last_patch = False

            msg = log_dur(var, tp, dt, last_patch, db)
            await ctx.send(msg)
            print("Done")
            db.close();


        @self.bot.command(name = 'dps', help = 'Get dps of a player in a phase/fight e.g. $dps "skorv, delay, Phase 2" -p')
        async def dps(ctx, *args):
            print(args)
            db = database.connect();
            var = [x.strip() for x in args[0].split(',')]
            try:
                tp = args[1]
            except:
                tp = 'a'
            try:
                dt = args[2]
            except:
                dt = ""
            try:
                last_patch = args[3]
            except:
                last_patch = False

            msg = log_dps(var, tp, dt, last_patch, db)
            await ctx.send(msg)
            print("Done")
            db.close();

        @self.bot.command(name = 'patches', help = 'Lists the latest big patch date')
        async def patches(ctx, *args):
            await ctx.send("11.5.2021.")

if __name__ == '__main__':
    bot = IV_Bot(TOKEN)
    bot.run()
