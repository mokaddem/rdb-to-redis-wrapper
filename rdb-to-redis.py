#!/usr/bin/env python3.5
# -*-coding:UTF-8 -*

import npyscreen, curses
import sys, os, time
import time, datetime
import argparse
import json
import redis
from subprocess import PIPE, Popen
import threading
import re

RDBOBJECT = None
RUNNING_REDIS_SERVER_NAME_COMMAND = rb"ps aux | grep redis-server | cut -d. -f4 | cut -s -d ' ' -f2 | grep :"
MEMORY_REPORT_COMMAND = r"rdb -c memory {}"
F = open('log', 'a')

op_type_mapping = {
        'SET':      'SADD',
        'STRING':   'SET',
        'ZSET':     'ZADD',
        'GEOSET':   'GEOADD',
        'HSET':     'HSET',
        'PFADD':    'HYPERLOGLOG',
        'LIST':     'LSET',
        }

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

class RDBObject:
    def __init__(self):
        self.filename   =   None
        self.target_server  =   {}
        self.target_server_type  =   {}
        self.target_server_indexes = []
        self.rdbDbs =           []      
        self.fileSize   =   0.0
        self.regexMaxSize=  0
        self.processStartTime = 0

        #after memory report (type change after MemReport)
        self.activeDB   =   '?'
        self.keyPerDB   =   '?'
        self.keyPerDBStr=   '?'
        self.totKey     =   '?'
        self.keyTypeCount=  {'string': '?', 'hash': '?', 'set': '?', 'sortedset': '?', 'list': '?'}
        self.keyTypeSizeCount=  {'string': '?', 'hash': '?', 'set': '?', 'sortedset': '?', 'list': '?'}
        self.sizeByDB   =   {}

    def execMemoryReport(self):
        if self.filename is None:
            npyscreen.notify_confirm("No RDB file selected", title='Please Wait', editw=1)
            return

        self.cmd = MEMORY_REPORT_COMMAND.format(self.filename)
        #estimate needed time
        self.estSecs = int(1.2*self.fileSize/(1024.0*1024.0)) #1.2s per Mb
        estTimeStr = "{:.2f} min".format(self.estSecs/60) if self.estSecs >= 60.0 else "{:.2f} sec".format(self.estSecs)

        start = npyscreen.notify_ok_cancel("Execute a memory report? This operation may take a long time.\nEstimated Time: ~{}".format(estTimeStr), title= 'Confirm', editw=1)
        if start:
            #set correct types
            self.activeDB   =   set()
            self.keyPerDB   =   {}
            self.totKey     =   0
            self.keyTypeCount=  {'string': 0, 'hash': 0, 'set': 0, 'sortedset': 0, 'list': 0}
            self.keyTypeSizeCount=  {'string': 0, 'hash': 0, 'set': 0, 'sortedset': 0, 'list': 0}
            self.totKeySize = 0

            self.processStartTime = time.time()
            ntfPopup    =   npyscreen.notify_confirm("Memory report in progress... \nResult will be saved in \'mem_report.txt\'.", title='Please Wait', editw=1)
            #Long command

            self.Pfinished = False
            self.memThread = threading.Thread(name='memThread', target=self.memReportFunction)
            self.memThread.setDaemon(True)
            self.memThread.start()

    def memReportFunction(self):
        self.process = Popen([self.cmd], stdin=PIPE, stdout=PIPE, bufsize=1, shell=True)

        report = self.process.stdout.read()
        report = report.decode('utf8')
        report = report.splitlines()
        for line in report[1:]:
            tab     =   line.split(',')
            db      =   int(tab[0])
            kType   =   tab[1]
            k       =   tab[2]
            size    =   tab[3]
            encod   =   tab[4]
            numElem =   tab[5]
            largElem=   tab[6]

            self.activeDB.add(db)
            if db not in self.keyPerDB:
                self.keyPerDB[db] = 0
                self.sizeByDB[db] = 0
            self.totKey += 1
            self.keyTypeCount[kType]    += 1
            self.keyPerDB[db] += 1
            self.keyTypeSizeCount[kType]+= int(size)/8
            self.totKeySize += int(size)/8
            self.sizeByDB[db] += int(size)/8
        elapsedTime = time.time() - self.processStartTime

        #cleanup
        for t, Bsize in self.keyTypeSizeCount.items():
            self.activeDB = list(self.activeDB)
            self.activeDB.sort()
            self.keyPerDBStr = "["
            for db in self.activeDB:
                self.keyPerDBStr += "{}: {}".format(db, self.keyPerDB[db])
                self.keyPerDBStr += ", "
            self.keyPerDBStr = self.keyPerDBStr[:-2]
            self.keyPerDBStr += "]"

        with open('mem_report.txt', 'w') as f:
            f.write(str(
                {
                    "filename": self.filename,
                    "File size":self.fileSize,
                    "active DB":self.activeDB,
                    "Total key":self.totKey,
                    "Number of key per key type": self.keyTypeCount,
                    "Total size per key type": self.keyTypeSizeCount
                }
                ))


        self.Pfinished = True

    def add_filename(self, filename):
        self.filename   =   filename
        self.fileSize   =   os.path.getsize(filename)
        self.regexMaxSize=  0

    def add_selected_db(self, dbs):
        self.rdbDbs = dbs

    def add_selected_key_type(self, serverList, keyTypeList):
        for serv in serverList:
            for typ in keyTypeList:
                if typ not in self.target_server_type[serv]:
                    self.target_server_type[serv].append(typ)

    def add_target_redis_servers(self, server_list):
        self.target_server = {}
        for serv in server_list:
            self.target_server[serv] = []
            self.target_server_type[serv] = []

    def add_target_redis_servers_indexes(self, index_list):
        self.target_server_indexes = index_list

    def add_regex_to_servers(self, regex, server_list):
        if len(server_list) > 0:
            self.regexMaxSize = self.regexMaxSize if self.regexMaxSize > len(regex) else len(regex)
        for serv in server_list:
            if regex not in self.target_server[serv]:
                self.target_server[serv].append(regex)

    def list_running_servers(self):
        p = Popen([RUNNING_REDIS_SERVER_NAME_COMMAND], stdin=PIPE, stdout=PIPE, bufsize=1, shell=True)
        return [serv.decode('ascii') for serv in p.stdout.read().splitlines()]

    def list_keyType(self):
        to_ret = [
                 "STRING",
                 "SET",
                 "ZSET",
                 "HSET",
                 "LIST",
                 "GEOSET",
                 "PFADD",
                ]
        return to_ret

    def get_activeDB(self):
        return self.activeDB

    def get_regexes_from_server(self, serv):
        to_ret = [r for r in self.target_server[serv]]
        return to_ret

    #return [0]: header, [1]: info 
    def get_rdb_infos(self):
        to_ret = []
        to_ret.append(['RDB file size', '# of active DB', 'Total # of Keys'])
        to_ret.append([(sizeof_fmt(self.fileSize), self.activeDB, self.totKey)])
        return to_ret

    def get_rdb_key_infos(self):
        to_ret = []
        to_ret.append(['', 'String', 'Hash', 'Set', 'Sortedset', 'List'])

        if self.totKey == 0 or self.totKey == '?':
            to_ret.append([
                ["Key type count:",
                    "{}".format(self.keyTypeCount['string']),
                    "{}".format(self.keyTypeCount['hash']),
                    "{}".format(self.keyTypeCount['set']),
                    "{}".format(self.keyTypeCount['sortedset']),
                    "{}".format(self.keyTypeCount['list'])
                ],
                ["Key type size:",
                    "{}".format(self.keyTypeSizeCount['string']),
                    "{}".format(self.keyTypeSizeCount['hash']),
                    "{}".format(self.keyTypeSizeCount['set']),
                    "{}".format(self.keyTypeSizeCount['sortedset']),
                    "{}".format(self.keyTypeSizeCount['list'])
                ]])
        else:
            to_ret.append([
                ["Key type count:",
                    "{}\t({:.2%})".format(self.keyTypeCount['string'], self.keyTypeCount['string']/self.totKey),
                    "{}\t({:.2%})".format(self.keyTypeCount['hash'], self.keyTypeCount['hash']/self.totKey),
                    "{}\t({:.2%})".format(self.keyTypeCount['set'], self.keyTypeCount['set']/self.totKey),
                    "{}\t({:.2%})".format(self.keyTypeCount['sortedset'], self.keyTypeCount['sortedset']/self.totKey),
                    "{}\t({:.2%})".format(self.keyTypeCount['list'], self.keyTypeCount['list']/self.totKey)
                ],
                ["Key type size:",
                    "{}\t({:.2%})".format(sizeof_fmt(self.keyTypeSizeCount['string']), self.keyTypeSizeCount['string']/self.totKeySize),
                    "{}\t({:.2%})".format(sizeof_fmt(self.keyTypeSizeCount['hash']), self.keyTypeSizeCount['hash']/self.totKeySize),
                    "{}\t({:.2%})".format(sizeof_fmt(self.keyTypeSizeCount['set']), self.keyTypeSizeCount['set']/self.totKeySize),
                    "{}\t({:.2%})".format(sizeof_fmt(self.keyTypeSizeCount['sortedset']), self.keyTypeSizeCount['sortedset']/self.totKeySize),
                    "{}\t({:.2%})".format(sizeof_fmt(self.keyTypeSizeCount['list']), self.keyTypeSizeCount['list']/self.totKeySize)
                ]
            ])

        return to_ret

    def get_rdb_DB_infos(self):
        to_ret = []
        db_str = [str(x) for x in self.sizeByDB.keys()]
        to_ret.append(['DB']+db_str)

        data1 = ["Size"]
        if len(self.sizeByDB) == 0:
            for db in self.get_16_db():
                data1.append('?')
        else:
            for db in self.sizeByDB.keys():
                try:
                    data1.append(sizeof_fmt(self.sizeByDB[db]))
                except KeyError: #no database
                    data1.append(sizeof_fmt(0.0))


        data2 = ["Keys"]
        if len(self.sizeByDB) == 0:
            for db in self.get_16_db():
                data2.append('?')
        else:
            for db in self.sizeByDB.keys():
                try:
                    data2.append(str(self.keyPerDB[db]))
                except KeyError: #no database
                    data1.append(sizeof_fmt(0.0))

        to_ret.append([data1, data2])
        return to_ret

    def get_servers_with_regex(self):
        return self.target_server

    def get_target_redis_servers(self):
        the_list = list(self.target_server.keys())
        if len(the_list) > 0:
            return the_list
        else:
            return self.list_running_servers()

    def get_target_redis_servers_indexes(self):
        return list(self.target_server_indexes)

    def get_selected_db(self):
        return self.rdbDbs

    def get_selected_type(self):
        return self.target_server_type

    def get_16_db(self):
        return [int(db) for db in range(16)]


'''
Screen 1
'''
class rdbForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def while_waiting(self):
        if RDBOBJECT.processStartTime != 0:
            self.time_widget.hidden = False
            self.timeR_widget.hidden = False
            self.time_pb.hidden = False
            self.grid_db.hidden = False
            self.grid2.hidden = False

            self.time_widget.display()
            self.timeR_widget.display()
            self.time_pb.display()

            while(not RDBOBJECT.Pfinished): #process not terminated
                elapsSec    =   time.time()-RDBOBJECT.processStartTime
                elapsTimeStr=   "{:.2f} min".format(elapsSec/60) if elapsSec >= 60.0 else "{:.2f} sec".format(elapsSec)
                remTimeStr=   "{:.2f} min".format(RDBOBJECT.estSecs - elapsSec/60) if RDBOBJECT.estSecs - elapsSec >= 60.0 else "{:.2f} sec".format(RDBOBJECT.estSecs - elapsSec)
                self.time_widget.value = elapsTimeStr
                self.timeR_widget.value = remTimeStr
                self.time_pb.value = elapsSec/RDBOBJECT.estSecs*100 if elapsSec/RDBOBJECT.estSecs*100 <= 100 else 99.99

                self.time_widget.display()
                self.timeR_widget.display()
                self.time_pb.display()
                time.sleep(1)

            RDBOBJECT.memThread.join()
            if elapsSec < 60:
                npyscreen.notify_confirm("Elapsed time: {:.2f} sec.\nResult saved in \'mem_report.txt\'.".format(float(elapsSec)), title= 'Info', editw=1)
            else:
                npyscreen.notify_confirm("Elapsed time: {:.2f} min.\nResult saved in \'mem_report.txt\'.".format(float(elapsSec)/60.0), title= 'Info', editw=1)

            self.time_widget.hidden = True
            self.timeR_widget.hidden = True
            self.time_pb.hidden = True

            rdbInfo             =   RDBOBJECT.get_rdb_infos()
            self.grid.values    =   rdbInfo[1]

            rdbInfoDB           =   RDBOBJECT.get_rdb_DB_infos()
            self.grid_db.col_titles=   rdbInfoDB[0]
            self.grid_db.values =   rdbInfoDB[1]

            rdbInfo             =   RDBOBJECT.get_rdb_key_infos()
            self.grid2.values   =   rdbInfo[1]

            self.chosenDb.values = RDBOBJECT.get_activeDB()
            self.chosenDb.value  = [x for x in range(len(self.chosenDb.values))]

            self.display()
            RDBOBJECT.processStartTime = 0


    def create(self):
        self.redisFile  =   self.add(npyscreen.TitleFilenameCombo, name="RDB file to process:")
        if RDBOBJECT.filename:
            self.redisFile.value = RDBOBJECT.filename
        self.redisFile.value_changed_callback = self.on_valueChanged
        self.vspace()

        self.btnMemory  =   self.add(npyscreen.ButtonPress, name="Generate memory report", when_pressed_function=RDBOBJECT.execMemoryReport)
        self.vspace()
        self.time_widget = self.add(npyscreen.TitleFixedText, name="Elapsed time:", value="", editable=False, hidden=True, color='STANDOUT') 
        self.timeR_widget = self.add(npyscreen.TitleFixedText, name="Estimated remaining time:", max_width=80, value="", editable=False, hidden=True, color='STANDOUT') 
        self.time_pb  = self.add(npyscreen.TitleSliderPercent, out_of=100, value=0, name="Estimated progress:", editable=False, hidden=True)
        self.vspace(1)

        rdbInfo = RDBOBJECT.get_rdb_infos()
        self.add(npyscreen.FixedText, value="RDB file information:", editable=False, color='LABEL')
        self.grid = self.add(npyscreen.GridColTitles, editable=False, columns=3, max_height=5, 
                col_titles=rdbInfo[0],
                values=rdbInfo[1])

        rdbInfoDB = RDBOBJECT.get_rdb_DB_infos()
        self.grid_db = self.add(npyscreen.GridColTitles, editable=False, max_height=5, hidden=True,
                col_titles=rdbInfoDB[0],
                values=rdbInfoDB[1])

        rdbInfo = RDBOBJECT.get_rdb_key_infos()
        self.grid2 = self.add(npyscreen.GridColTitles, editable=False, columns=6, max_height=5, hidden=True,
                col_titles=rdbInfo[0],
                values=rdbInfo[1])

        self.vspace(3)
        self.chosenDb   =   self.add(npyscreen.TitleMultiSelect, max_height=15+3, max_width=30,
                name    =   "Select DB Number from file:", 
                value   =   RDBOBJECT.get_selected_db(), 
                values  =   RDBOBJECT.get_16_db())
        self.vspace()
        self.chosenServer=  self.add(npyscreen.TitleMultiSelect, max_height=10, rely=32, relx=self.chosenDb.width+10,
                name    =   "Select Redis server in which to inject:", 
                value   =   RDBOBJECT.get_target_redis_servers_indexes(), 
                values  =   RDBOBJECT.list_running_servers())

    def on_valueChanged(self, *args, **keywords):
        if self.redisFile.value:
            RDBOBJECT.add_filename(self.redisFile.value)
            rdbInfo = RDBOBJECT.get_rdb_infos()
            self.grid.values = rdbInfo[1]
            self.grid.display()

    def on_ok(self):
        if not self.redisFile.value: #Do not accept no server
            npyscreen.notify_confirm("RDB file not selected", title='Error', editw=1)
            return
        selected = self.chosenServer.get_selected_objects()
        if not selected: #Do not accept no server
            npyscreen.notify_confirm("No redis server server selected", title='Error', editw=1)
            return
        RDBOBJECT.add_target_redis_servers(selected)
        RDBOBJECT.add_target_redis_servers_indexes(self.chosenServer.value)
        selected = self.chosenDb.get_selected_objects()
        if not selected: #Do not accept no db
            npyscreen.notify_confirm("No database selected", title='Error', editw=1)
            return
        RDBOBJECT.add_selected_db(selected)
        self.parentApp.switchForm('FILTER')


    def on_cancel(self):
        sys.exit(0)


'''
Screen 2
'''
class filterForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        self.chosenType=  self.add(npyscreen.TitleMultiSelect, max_height=10, max_width=40,
                name    =   "Select type from the RDB file to be imported:", 
                value   =   [x for x in range(len(RDBOBJECT.list_keyType()))], 
                values  =   RDBOBJECT.list_keyType())
        self.vspace()

        self.regex  =   self.add(npyscreen.TitleText, name='Regex:', value='')
        self.vspace(2)
        self.add(npyscreen.TitleFixedText, name='Select redis database for which this regex applies:', 
                value='', editable=False)
        self.tree   =   self.add(npyscreen.MLTreeMultiSelect, max_height=30, max_width=80)
        treedata    =   npyscreen.NPSTreeData(content='Redis servers', selectable=False, ignoreRoot=False)
        for serv in RDBOBJECT.get_target_redis_servers():
            n = treedata.newChild(content=serv, selectable=True)
            for r in RDBOBJECT.get_regexes_from_server(serv):
                n.newChild(content=r, selectable=False)
            n.newChild(content=RDBOBJECT.get_selected_type()[serv], selectable=False)

        self.tree.values = treedata

        self.add_button = self.add(npyscreen.ButtonPress, name = 'Add', relx = self.tree.width+10, rely = 19)
        self.add_button.whenPressed = self.addRegType


    def addRegType(self):
        #regex
        selected = self.tree.get_selected_objects(return_node=False)
        selected =[x for x in selected]
        regVal = self.regex.value
        if regVal is "":
            regVal = "(.*?)" #match anything

        RDBOBJECT.add_regex_to_servers(regVal, selected)

        #type
        selected_type = self.chosenType.get_selected_objects()
        if not selected_type: #Do not accept no db
            npyscreen.notify_confirm("No key type selected", title='Error', editw=1)
            return
        RDBOBJECT.add_selected_key_type(selected, selected_type)

        treedata    =   npyscreen.NPSTreeData(content='Redis servers', selectable=False, ignoreRoot=False)
        for serv in RDBOBJECT.get_target_redis_servers():
            n = treedata.newChild(content=serv, selectable=True)
            for r in RDBOBJECT.get_regexes_from_server(serv):
                n.newChild(content=r, selectable=False)
            n.newChild(content=RDBOBJECT.get_selected_type()[serv], selectable=False)

        self.tree.values = treedata
        self.regex.value = ''
        self.tree.display()

    def on_cancel(self):
        self.parentApp.switchForm('MAIN')

    def on_ok(self):
        self.parentApp.switchForm('CONFIRM')

'''
Screen 3
'''
class confirmForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        curY = 0
        self.rdbFile = self.add(npyscreen.BoxTitle, name='RDB file', relx=2, max_height=8, editable=False,
                values=["Database to be processed:", "    "+str(RDBOBJECT.get_selected_db())]
                )
        self.vspace(2)
        curY += 11

        i=0
        for serv in RDBOBJECT.get_target_redis_servers():
            box = self.add(npyscreen.BoxTitle, name=serv, relx=2, max_height=7, editable= False)
            box.values = RDBOBJECT.get_regexes_from_server(serv)+[str(RDBOBJECT.get_selected_type()[serv])]
            i += 1

        curY += 17


    def on_cancel(self):
        self.parentApp.switchForm('FILTER')

    def on_ok(self):
        self.parentApp.setNextForm(None)

class MyApplication(npyscreen.NPSAppManaged):
    keypress_timeout_default = 10 

    def onStart(self):
        self.addFormClass('MAIN', rdbForm, name='RDB to Redis Server')
        self.addFormClass('FILTER', filterForm, name='Filter')
        self.addFormClass('CONFIRM', confirmForm, name='Confirm')
        #self.addForm('MAIN', filterForm, name='Filtering')


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read rdb file and copy wanted content into a running redis server.')
    parser.add_argument('-f', '--filename', required=False, dest='rdbFile', help='The RDB file from which data must be copied')
    parser.add_argument('-d', '--db', required=False, type=str, default='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15', dest='redisDb', help='The RDB databases number in which data must be taken')
    parser.add_argument('-r', '--redisServer', required=False, type=str, dest='redisServer', help='The redis server in which data must be copied')

    args = parser.parse_args()

    RDBOBJECT = RDBObject()
    if args.rdbFile:
        RDBOBJECT.add_filename(args.rdbFile)
    if args.redisServer:
        args.redisServer = [int(item) for item in args.redisServer.split(',')]
        RDBOBJECT.add_target_redis_servers(args.redisServer)
    if args.redisDb:
        args.redisDb = [int(item) for item in args.redisDb.split(',')]
        int_db = [int(db) for db in args.redisDb]
        RDBOBJECT.add_selected_db(int_db)

    App = MyApplication()
    App.run()
    inject(RDBOBJECT.filename, RDBOBJECT.get_servers_with_regex(), RDBOBJECT.get_selected_type(), RDBOBJECT.get_selected_db(), True)

    '''
    filename = "dump6382.rdb"
    inject(
    filename,
    {'*:8888': ['TopTermFreq_set_day_'], '*:8889': ['per_paste_']},
    {'*:8888': ['STRING', 'SET', 'ZSET', 'HSET', 'LIST', 'GEOSET', 'PFADD'], '*:8889': ['STRING', 'SET', 'ZSET', 'HSET', 'LIST', 'GEOSET', 'PFADD']},
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    True)
    '''
