import pymysql.cursors
from variables import *
import log_parser
import time
import datetime

def connect():
    db = pymysql.connect(
        host        = DB_HOST,
        user        = DB_USERNAME,
        password    = DB_PASSWORD,
        database    = DB_NAME,
        charset     = "utf8mb4",
        cursorclass = pymysql.cursors.DictCursor
    )
    
    print("[*] Connected to " , DB_NAME)
    return db

def levenshteinDistance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_

    return distances[-1]

def is_duplicate(log_data, cursor):
    log_date = datetime.datetime.strptime(log_data['encounterStart'], '%Y-%m-%d %H:%M:%S %z')
    utc = datetime.timezone(datetime.timedelta())
    log_date = log_date.astimezone(utc)
    t_range = datetime.timedelta(seconds = 2)
    sql = "SELECT id FROM log_table WHERE logDate > '%s' AND logDate < '%s'"\
          % (log_date - t_range, log_date + t_range)
    cursor.execute(sql)
    return len(cursor.fetchall()) > 0

def get_phase_id(db, phase_name_id, log_id, cursor):
    # I did this seperate so I can link phase to boss and need two IF statements 
    sql = "SELECT id from phase_table WHERE phaseNameID = %s AND logID = %s" % (phase_name_id, log_id);
    cursor.execute(sql)
    tmp = cursor.fetchall()
    return tmp[0]['id']

def get_name_id(db, table_name, var, name, cursor):
    # Get the appropriate id of the boss/phase/player, if it doesn't exist (i.e. first entry)
    # then insert it into the table and get its id
    t0 = time.time()    
    sql = "SELECT id from %s WHERE %s = '%s'" % (table_name, var, name)
    cursor.execute(sql)
    tmp = cursor.fetchall()
    if (len(tmp) == 0):
        cursor.execute("INSERT INTO %s (%s) VALUES ('%s')" % (table_name, var, name))
        cursor.execute(sql)
        tmp = cursor.fetchall()

    t1 = time.time()
    #print(t1 - t0)
    return tmp[0]['id']

def get_exact_id(table_name, var, name, cursor):
    # Get list of all bosses
    sql = "SELECT ID, %s FROM %s" % (var, table_name)
    cursor.execute(sql)
    return sorted([(levenshteinDistance(name, x[var]), x['ID']) for x in cursor.fetchall()])[0][1]

if __name__ == "__main__":
    db = connect()
    log = "https://dps.report/FXTr-20210821-145348_arriv"
    log = "https://dps.report/QVVU-20210906-173838_skor" 
    log = "https://dps.report/lLj0-20210905-165628_arkk" # FAILED LOG

    var = ["ark", "full", "Phase 3", "delay/slb"]
    tp = 'c'

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
            if '/' not in var[3]:
                play_name_id = get_exact_id("player_name_table", "playerName", tmp[0], cursor)
                class_name_id = ""
            else:
                tmp = var[3].split('/')
                player_name_id = "" if tmp[0] == "" else get_exact_id("player_name_table", "playerName", tmp[0], cursor)
                class_name_id = "" if tmp[1] == "" else get_exact_id("class_name_table", "className", tmp[1], cursor)

            boss_name_id = get_exact_id("boss_name_table", "bossName", var[0], cursor)
            phase_name_id = get_exact_id("phase_name_table", "phaseName", var[2], cursor)
            sql =   "SELECT l.Log, d.%s " \
                    "FROM log_table AS l " \
                    "INNER JOIN phase_table AS p ON l.ID = p.logID " \
                    "INNER JOIN dps_table AS d ON p.ID = d.phaseID " \
                    "WHERE l.bossNameID = %s AND p.phaseNameID = %s " \
                    % (d[var[1]], boss_name_id, phase_name_id)  
            
            if player_name_id != "":
                sql += "AND d.playerNameID = %s " % player_name_id

            if class_name_id != "":
                sql += "AND d.classNameID = %s " % class_name_id

            if tp != 'a':
                    sql += "AND l.logClass = %s " % (tp == 'c')

            sql += "ORDER BY d.%s DESC LIMIT 20" % (d[var[1]])

            cursor.execute(sql)
            tmp = cursor.fetchall()
            msg = "\n".join([" ".join([x['Log'], str(x[d[var[1]]])]) for x in tmp])
            
        except Exception as e:
            print("Error when fetching log, exception ", e)
            msg = "Your query was bad you monkey, %s" % e


    print(msg)
    db.commit()