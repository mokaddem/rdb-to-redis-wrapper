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

RDBOBJECT = None
RUNNING_REDIS_SERVER_NAME_COMMAND = rb"ps aux | grep redis-server | cut -d. -f4 | cut -s -d ' ' -f2 | grep :"
MEMORY_REPORT_COMMAND = r"rdb -c memory {}"
F = open('log', 'a')

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
        self.target_server_indexes = []
        self.rdbDbs =           []      
        self.fileSize   =   0.0
        self.regexMaxSize=  0
        self.processStartTime = 0

        #after memory report (type change after MemReport)
        self.activeDB   =   '?'
        self.totKey     =   '?'
        self.keyTypeCount=  {'string': '?', 'hash': '?', 'set': '?', 'sortedset': '?', 'list': '?'}
        self.keyTypeSizeCount=  {'string': '?', 'hash': '?', 'set': '?', 'sortedset': '?', 'list': '?'}

    def execMemoryReport(self):
        self.cmd = MEMORY_REPORT_COMMAND.format(self.filename)
        #estimate needed time
        self.estSecs = int(1.2*self.fileSize/(1024.0*1024.0)) #1.2s per Mb
        estTimeStr = "{:.2f} min".format(self.estSecs/60) if self.estSecs >= 60.0 else "{:.2f} sec".format(self.estSecs)

        start = npyscreen.notify_ok_cancel("Execute a memory report? This operation may take a long time.\nEstimated Time: ~{}".format(estTimeStr), title= 'Confirm', editw=1)
        if start:
            #set correct types
            self.activeDB   =   set()
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
            db      =   tab[0]
            kType   =   tab[1]
            k       =   tab[2]
            size    =   tab[3]
            encod   =   tab[4]
            numElem =   tab[5]
            largElem=   tab[6]

            self.activeDB.add(db)
            self.totKey += 1
            self.keyTypeCount[kType]    += 1
            self.keyTypeSizeCount[kType]+= int(size)/8
            self.totKeySize += int(size)/8
        elapsedTime = time.time() - self.processStartTime

        #cleanup
        for t, Bsize in self.keyTypeSizeCount.items():
            self.activeDB = list(self.activeDB)
            self.activeDB.sort()

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

    def add_target_redis_servers(self, server_list):
        self.target_server = {}
        for serv in server_list:
            self.target_server[serv] = []

    def add_target_redis_servers_indexes(self, index_list):
        self.target_server_indexes = index_list

    def add_regex_to_servers(self, regex, server_list):
        if len(server_list) > 0:
            self.regexMaxSize = self.regexMaxSize if self.regexMaxSize > len(regex) else len(regex)
        for serv in server_list:
            self.target_server[serv].append(regex)

    def list_running_servers(self):
        p = Popen([RUNNING_REDIS_SERVER_NAME_COMMAND], stdin=PIPE, stdout=PIPE, bufsize=1, shell=True)
        return [serv.decode('ascii') for serv in p.stdout.read().splitlines()]

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
        data = []

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

    def get_target_redis_servers(self):
        the_list = list(self.target_server.keys())
        if len(the_list) > 0:
            return the_list
        else:
            return self.list_running_servers()

    def get_target_redis_servers_indexes(self):
        return list(self.target_server_indexes)

    def get_seleced_db(self):
        return self.rdbDbs

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
            self.time_widget.display()
            self.timeR_widget.display()
            self.time_pb.display()

            rdbInfo             =   RDBOBJECT.get_rdb_infos()
            self.grid.values    =   rdbInfo[1]
            rdbInfo             =   RDBOBJECT.get_rdb_key_infos()
            self.grid2.values   =  rdbInfo[1]
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
        self.timeR_widget = self.add(npyscreen.TitleFixedText, name="Estimated remaining time:", max_width=30, value="", editable=False, hidden=True, color='STANDOUT') 
        self.time_pb  = self.add(npyscreen.TitleSliderPercent, out_of=100, value=0, name="Estimated progress:", editable=False, hidden=True)
        self.vspace(1)

        rdbInfo = RDBOBJECT.get_rdb_infos()
        self.add(npyscreen.FixedText, value="RDB file information:", editable=False, color='LABEL')
        self.grid = self.add(npyscreen.GridColTitles, editable=False, columns=3, max_height=5,
                col_titles=rdbInfo[0],
                values=rdbInfo[1])
        #self.vspace()

        rdbInfo = RDBOBJECT.get_rdb_key_infos()
        self.grid2 = self.add(npyscreen.GridColTitles, editable=False, columns=6, max_height=5,
                col_titles=rdbInfo[0],
                values=rdbInfo[1])

        self.vspace(3)
        self.chosenDb   =   self.add(npyscreen.TitleMultiSelect, max_height=15+3, max_width=30,
                name="Select DB Number from file:", 
                value   =   RDBOBJECT.get_seleced_db(), 
                values  =   RDBOBJECT.get_16_db())
        self.vspace()
        self.chosenServer=  self.add(npyscreen.TitleMultiSelect, max_height=10, rely=27, relx=self.chosenDb.width+10,
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
        selected = self.chosenServer.get_selected_objects()
        RDBOBJECT.add_target_redis_servers(selected)
        RDBOBJECT.add_target_redis_servers_indexes(self.chosenServer.value)
        selected = self.chosenDb.get_selected_objects()
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
        self.regex  =   self.add(npyscreen.TitleText, name='Regex:', value='')
        self.vspace(2)
        self.add(npyscreen.TitleFixedText, name='Select redis database for which this regex applies:', 
                value='', editable=False)
        self.tree   =   self.add(npyscreen.MLTreeMultiSelect)
        treedata    =   npyscreen.NPSTreeData(content='Redis servers', selectable=False, ignoreRoot=False)
        for serv in RDBOBJECT.get_target_redis_servers():
            n = treedata.newChild(content=serv, selectable=True)
            for r in RDBOBJECT.get_regexes_from_server(serv):
                n.newChild(content=r, selectable=False)

        self.tree.values = treedata

        self.add_button = self.add(npyscreen.ButtonPress, name = 'Add', relx = 20,rely = 25)
        self.add_button.whenPressed = self.addReg


    def addReg(self):
        selected = self.tree.get_selected_objects(return_node=False)
        regVal = self.regex.value
        RDBOBJECT.add_regex_to_servers(regVal, [x for x in selected])

        treedata    =   npyscreen.NPSTreeData(content='Redis servers', selectable=False, ignoreRoot=False)
        for serv in RDBOBJECT.get_target_redis_servers():
            n = treedata.newChild(content=serv, selectable=True)
            for r in RDBOBJECT.get_regexes_from_server(serv):
                n.newChild(content=r, selectable=False)

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
        self.rdbFile = self.add(npyscreen.BoxBasic, name='RDB file', max_width=20, relx=2, max_height=15, editable=False)
        self.vspace(2)
        curY += 17+2

        boxsize = RDBOBJECT.regexMaxSize+8
        i=0
        for serv in RDBOBJECT.get_target_redis_servers():
            if boxsize < 40:
                box = self.add(npyscreen.BoxTitle, name=serv, max_width=boxsize, rely=curY, relx=2+i*boxsize, max_height=15, editable= False)
            else:
                box = self.add(npyscreen.BoxTitle, name=serv, max_width=boxsize, relx=2, max_height=10, editable= False)

            box.values = RDBOBJECT.get_regexes_from_server(serv)
            i += 1

        #self.box1 = self.add(npyscreen.BoxTitle, name='*:8888', max_width=20, relx=2, max_height=15, editable=False)
        #self.box2 = self.add(npyscreen.BoxTitle, name='*:6379', max_width=20, rely=curY, relx=22, max_height=15, editable= False,
        #        contained_widget_arguments={
        #            })
        curY += 17
        #self.box2.values = ["regex1", "regex2"]


    def on_cancel(self):
        self.parentApp.switchForm('FILTER')

    def on_ok(self):
        sys.exit(1)

class MyApplication(npyscreen.NPSAppManaged):
    keypress_timeout_default = 10 

    def onStart(self):
        self.addFormClass('MAIN', rdbForm, name='RDB to Redis Server')
        self.addFormClass('FILTER', filterForm, name='Filter')
        self.addFormClass('CONFIRM', confirmForm, name='Confirm')
        #self.addForm('MAIN', filterForm, name='Filtering')

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
        RDBOBJECT.add_selected_db(args.redisDb)

    App = MyApplication()
    App.run()

    # REDIS #
    #servers = []
    #for i in range(15):
    #    server = redis.StrictRedis(
    #        host='localhost',
    #        port=args.redisPort,
    #        db=i)
    #    servers.append(server)


''' WIDGETS '''
        #ml = F.add(npyscreen.MultiLineEdit, value = """try typing here!\nMutiline text, press ^R to reformat.\n""",

        #self.tree   =   self.add(npyscreen.MLTreeMultiSelect)
        #treedata = npyscreen.NPSTreeData(content='Root', selectable=True,ignoreRoot=False)
        #c1 = treedata.newChild(content='Child 1', selectable=True, selected=True)
        #c2 = treedata.newChild(content='Child 2', selectable=True)
        #g1 = c1.newChild(content='Grand-child 1', selectable=True)
        #g2 = c1.newChild(content='Grand-child 2', selectable=True)
        #g3 = c1.newChild(content='Grand-child 3')
        #gg1 = g1.newChild(content='Great Grand-child 1', selectable=True)
        #gg2 = g1.newChild(content='Great Grand-child 2', selectable=True)
        #gg3 = g1.newChild(content='Great Grand-child 3')
        #self.tree.values = treedata
       #       max_height=5, rely=9)

        #s  = self.add(npyscreen.TitleSliderPercent, out_of=100, value=35, name="Progress:", editable=False)

