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

def edit_distance(s1, s2):
    # weighted edit distance - deletion should cost more and assume s1 has similar letters to s2 near the start 
    s1 = s1.lower()
    s2 = s2.upper()
    m = len(s1)
    n = len(s2)
    cost_delete = 100
    cost_substitute = 50
    d = [[cost_delete*i] for i in range(1, m + 1)]   # d matrix rows
    d.insert(0, [0] + list(range(20, n + 20)))   # d matrix columns
    for j in range(n):
        for i in range(m):
            cost_insert = m - i
            if s1[i].lower() == s2[j].lower():   # Python (string) is 0-based
                cost = 0
            else:
                cost = cost_substitute
            d[i+1].insert(j+1, min(d[i][j+1] + cost_delete,
                                   d[i+1][j] + cost_insert,
                                   d[i][j] + cost))
    return d[-1][-1]

def is_duplicate(log_data, cursor):
    log_date = datetime.datetime.strptime(log_data['encounterStart'], '%Y-%m-%d %H:%M:%S %z')
    utc = datetime.timezone(datetime.timedelta())
    log_date = log_date.astimezone(utc)
    t_range = datetime.timedelta(seconds = 10)
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

def get_name_id(table_name, var, name, cursor):
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
    return sorted([(edit_distance(name, x[var]), x['ID']) for x in cursor.fetchall()])[0][1]

def insert_log(log, log_date, log_dur, log_class, success, boss_name, cursor):
    # Insert into Log table
    
    #get boss_name_id
    boss_name_id = get_name_id("boss_name_table", "bossName", boss_name, cursor)
    
    sql = "INSERT INTO log_table (log, logDate, logDuration, logClass, success, bossNameID) VALUES ('%s', '%s', %s, %s, %s, %s)" \
        % (log, log_date, log_dur, log_class, success, boss_name_id) 
    
    cursor.execute(sql)
    return cursor.lastrowid;

def insert_phases(log_data, log_id, success, cursor):
    # Insert all phases into phase table 
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
        phase_name_id = get_name_id("phase_name_table", "phaseName", phase_name, cursor)
        phase_list.append("(%s, %s, %s, %s, %s)"  % (log_id, phase_name_id, start, end, phase_dur))
        starts.append(start)
        ends.append(end)

    sql += ",".join(phase_list) + ";"
    cursor.execute(sql)
    return cursor.lastrowid, len(phase_list), starts, ends
    
def insert_players(log_data, phase_id_last, phase_count, starts, ends, cursor):
    # Insert all player information into player table 
    player_ids  = []
    class_ids   = []
    for j in range(len(log_data['players'])):
        player_ids.append(get_name_id("player_name_table", "playerName", log_data['players'][j]['acc'], cursor))
        class_ids.append(get_name_id("class_name_table", "className", log_data['players'][j]['profession'], cursor))

    sql = "INSERT INTO dps_table (phaseID, playerNameID, classNameID, startDPS, endDPS, phaseDPS) VALUES "
    dps_list = []

    # For each player save the sql entry then insert all at once
    for phase_id, start, end in zip(range(phase_id_last, phase_id_last + phase_count), starts, ends):    
        #phase_id = get_phase_id(db, phase_name_id, log_id, cursor)
        for j in range(len(log_data['players'])):
            # j - num of player

            player_name, class_name, startDPS, endDPS, phaseDPS = log_parser.get_player_insert_info(log_data, j, start, end)
            player_name_id = player_ids[j]
            class_name_id  = class_ids[j]
            dps_list.append("(%s, %s, %s, %s, %s, %s)" % (phase_id, player_name_id, class_name_id, startDPS, endDPS, phaseDPS))


    sql += ",".join(dps_list) + ";"
    cursor.execute(sql)


if __name__ == "__main__":
    db = connect()
    log = "https://dps.report/FXTr-20210821-145348_arriv"
    log = "https://dps.report/QVVU-20210906-173838_skor" 
    log = "https://dps.report/lLj0-20210905-165628_arkk" # FAILED LOG
    # log = "https://dps.report/Gtno-20210830-180153_arkk"

    var = ["arkk", "full", "Phase 3", "delay/slb"]
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

        print("NAREDBA\n", sql)
        cursor.execute(sql)
        tmp = cursor.fetchall()
        msg = "\n".join([" ".join([x['Log'], str(x[d[var[1]]])]) for x in tmp])
        

    print(msg)
    db.commit()