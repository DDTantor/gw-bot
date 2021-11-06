import discord
import database
import time
import log_parser
import bot_commands
from discord.ext import commands
from variables import *

TOKEN = DISCORD_TOKEN

def convert_input(args):
    # returns body and flags
    
    body = [x.strip() for x in args[0].split(',')]
    
    it = iter(args[1:])
    flags = dict(zip(it, it))
    tp = (flags['-c'] if bool(flags.get('-c')) else "a")
    dt = (flags['-d'] if bool(flags.get('-d')) else "")
    return body, tp, dt

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
                bot_commands.upload_log_command(log_link, db)
                t1 = time.time()
                print("Log inserted")
                print(t1 - t0)
                if i % 5 == 0:
                    db.commit()

            print("Done")
            await ctx.send("Done inserting logs")
            db.close()

        @self.bot.command(name = 'dur', help = 'Get duration of phases/fights e.g. $dur "skorv, end, Phase 2" -c a -d 2021-08-22')
        async def dur(ctx, *args):
            # Here we pray people wont write stupid commands
            # BODY
            # - boss
            # - type: {start, end, full}
            #          start - when does phase start in log
            #          end   - when does phase end in log 
            #          full  - duration of the phase 
            # - phase
            # FLAGS
            # -d (ex. -d 2021-08-24 16:28:13) queries logs after inserted date
            # -c (ex. -c {type}) queries logs of type {c = condi, p = power}
            #
 
            db = database.connect();
            
            body, tp, dt = convert_input(args)
            print("BODY FOR QUERY:", body)
            print("FLAGS FOR QUERY: log_type {} log_date {}".format(tp, dt))

            msg = bot_commands.log_dur_command(body, tp, dt, db)
            await ctx.send(msg)
            print("Done")
            db.close();


        @self.bot.command(name = 'dps', help = 'Get dps of a player in a phase/fight e.g. $dps "skorv, full, Phase 2, delay/slb" -c p -d 2021-08-22')
        async def dps(ctx, *args):
            db = database.connect();
            
            body, tp, dt = convert_input(args)
            print("BODY FOR QUERY:", body)
            print("FLAGS FOR QUERY: log_type {} log_date {}".format(tp, dt))

            msg = bot_commands.log_dps_command(body, tp, dt, db)
            await ctx.send(msg)
            print("Done")
            db.close();

        @self.bot.command(name = 'patches', help = 'Lists the latest big patch date')
        async def patches(ctx, *args):
            await ctx.send("11.5.2021.")

if __name__ == '__main__':
    bot = IV_Bot(TOKEN)
    bot.run()
