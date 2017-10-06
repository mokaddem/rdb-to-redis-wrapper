import time
from datetime import timedelta
import os
import re
from subprocess import PIPE, Popen

op_type_mapping = {
        'SET':      'SADD',
        'STRING':   'SET',
        'ZSET':     'ZADD',
        'GEOSET':   'GEOADD',
        'HSET':     'HSET',
        'HYPERLOGLOG':    'PFADD',
        'LIST':     'LSET',
        }

def inject(filename, serv_to_reg, serv_to_type, db_list, keep_db_organsization):
    cmd = ["rdb","--c", "protocol", filename]
    for db in db_list:
        cmd += ["--db"]
        cmd += [str(db)]

    p_rdb=Popen(cmd, stdin=PIPE, stdout=PIPE)
    p_cli_tab = {}

    #compile reg, inverse dico reg
    reg_to_serv = {}
    compiled_reg= {}
    for serv, reg_list in serv_to_reg.items():
        hostname, port = serv.split(':')
        p_cli_tab[serv] = Popen(["../redis/src/redis-cli","--pipe", "-h", hostname, "-p", port], stdin=PIPE, stdout=PIPE)
        for reg in reg_list:
            if reg not in reg_to_serv:
                reg_to_serv[reg] = []
                compiled_reg[reg] = re.compile(reg)
            reg_to_serv[reg].append(serv)

    #inverse dico type
    op_type_to_serv = {}
    for serv, type_list in serv_to_type.items():
        for typ in type_list:
            type_mapped = op_type_mapping[typ]
            if type_mapped not in op_type_to_serv:
                op_type_to_serv[type_mapped] = []
            op_type_to_serv[type_mapped].append(serv)


    filesize = os.path.getsize(filename)
    sec_per_mb = 0.6
    if filesize/(1024.0*1024.0) < 200.0:
        sec_per_mb *= 2
    estSecs = int(sec_per_mb*filesize/(1024.0*1024.0)) #s per Mb
    print("Estimated process duration: {}".format(str(timedelta(seconds=estSecs))))

    ##Process the file
    time_s = time.time()
    last_updated = 0.0
    cur_db_num = 0
    processed_key = 0
    inject_key = 0
    for arg in p_rdb.stdout:
        to_send = b''
        #arg = p_rdb.stdout.readline()
        to_send += arg
        left = int(arg[1:])*2 #remaining num of line

        if '*2' in arg.decode():

            for x in range(3):#consume 3 time
                to_send += p_rdb.stdout.readline()

            cur_db_num = p_rdb.stdout.readline()
            to_send += cur_db_num
            cur_db_num = int(cur_db_num[:-2])
            left -= 4

            #change DB num
            if keep_db_organsization:
                for i in range(left):
                    to_send += p_rdb.stdout.readline()

                #change db on all server
                for serv in serv_to_type.keys():
                    p_cli_tab[serv].stdin.write(to_send)
                    p_cli_tab[serv].stdin.flush()
                continue
            else:
                continue #skip db


        #consume lines
        to_send += p_rdb.stdout.readline() # length
        left -= 1

        op_type = p_rdb.stdout.readline() 
        to_send += op_type
        op_type = op_type[:-2].decode()
        left -= 1
        to_send += p_rdb.stdout.readline() # length
        left -= 1
        key = p_rdb.stdout.readline() 
        to_send += key
        key = key[:-2].decode()
        left -= 1

        for i in range(left):
            to_send += p_rdb.stdout.readline()

        #filter & redirect
        #Apply key type
        processed_key += 1
        if op_type in op_type_to_serv:
            for serv in op_type_to_serv[op_type]:

                #Apply regex
                if len(compiled_reg) == 0: #no regex provided, send anyway
                    for serv in serv_to_reg.keys():
                        p_cli_tab[serv].stdin.write(to_send)
                        inject_key += 1

                else:
                    for regName, regComp in compiled_reg.items():
                        if regComp.search(key): #if 1 and only 1 match
                            #Apply redirect
                            for serv in reg_to_serv[regName]:
                                p_cli_tab[serv].stdin.write(to_send)
                            inject_key += 1

        #Print progress
        now = time.time()
        duration = now - time_s
        if now - last_updated >= 1.0:
            last_updated = now
            duration_str = "{:.2f} min".format(duration/60) if duration >= 60.0 else "{:.2f} sec".format(duration)
            print('Current DB: {}, Elapsed time: {}, Processed key: {}, Injected item: {}'.format(cur_db_num, duration_str, processed_key, inject_key), sep=' ', end='\r', flush=True)

    else:
        duration = time.time()-time_s
        print()
        print('Data injected. Duration: {:.2f}s ({:.2f}min)'.format(duration, duration/60))



