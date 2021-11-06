import discord
import database
import time
import log_parser
import bot_commands
import pandas
        
if __name__ == '__main__':
    fl = pandas.read_csv(r"log_table.csv")
    db = database.connect();
    cnt = 0;
    total_time = 0;
    for i, log_link in enumerate(fl["Log"]):
        t0 = time.time()
        print("Inserting {}".format(log_link))
        bot_commands.upload_log_command(log_link, db)
        t1 = time.time()
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