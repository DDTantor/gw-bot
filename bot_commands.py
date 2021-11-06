import database
import log_parser
from variables import LOG_DISCARD_TIME

def upload_log_command(log, db):
    try:
        log_data = log_parser.get_log_data(log)
    except Exception as e:
        print("Error when getting log info, exception ", e)    
    try:
        with db.cursor() as cursor:
            
            log_date, log_dur, log_class, success, boss_name = log_parser.get_log_insert_info(log_data) 
            # check if log already exists in db based on time
            if float(log_dur) < LOG_DISCARD_TIME or database.is_duplicate(log_data, cursor):
                return 
            
            # Enter the log into the log_table and return log id
            log_id = database.insert_log(log, log_date, log_dur, log_class, success, boss_name, cursor)
 
            # Enter phases into the phase_table and return last_phase_id
            last_phase_id, phase_count, starts, ends = database.insert_phases(log_data, log_id, success, cursor)
            
            # Enter players into the player_table
            database.insert_players(log_data, last_phase_id, phase_count, starts, ends, cursor)
            

    except Exception as e:
        print("Error when inserting log, exception ", e)
        
def log_dur_command(var, tp, dt, db):
    #  $dur "skorv, end, Phase 2" -c p -d 2021-08-22
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
    
def log_dps_command(var, tp, dt, db):
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

            cursor.execute(sql)
            tmp = cursor.fetchall()
            msg = "\n".join([" ".join([x['Log'], str(x[d[var[1]]])]) for x in tmp])
            
        except Exception as e:
            print("Error when fetching log, exception ", e)
            msg = "Your query was bad you monkey, %s" % e

    return msg

if __name__ == '__main__':
	pass