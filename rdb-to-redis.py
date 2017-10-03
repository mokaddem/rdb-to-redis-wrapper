#!/usr/bin/env python3.5
# -*-coding:UTF-8 -*

import npyscreen, curses
import sys, os
import time, datetime
import argparse
import json
import redis
from subprocess import PIPE, Popen

RDBOBJECT = None
RUNNING_REDIS_SERVER_NAME_COMMAND = rb"ps aux | grep redis-server | cut -d. -f4 | cut -s -d ' ' -f2 | grep :"
F = open('log', 'w')

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

class RDBObject:
    def __init__(self):
        self.target_server  =   {}
        self.rdbDbs =           []      
        self.fileSize   =   ''
        self.numDB      =   ''
        self.numKeys    =   ''
        self.regexMaxSize=  0


    def add_filename(self, filename):
        self.filename   =   filename
        self.fileSize   =   sizeof_fmt(os.path.getsize(filename))
        self.numDB      =   0
        self.numKeys    =   0
        self.regexMaxSize=  0

    def add_selected_db(self, dbs):
        self.rdbDbs = dbs

    def add_target_redis_servers(self, server_list):
        self.target_server = {}
        for serv in server_list:
            self.target_server[serv] = []

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
        to_ret.append(['RDB size', '# of active DB', 'Total # of Keys'])
        to_ret.append([(self.fileSize, self.numDB, self.numKeys)])
        return to_ret

    def get_target_redis_servers(self):
        return list(self.target_server.keys())

    def get_seleced_db(self):
        return self.rdbDbs


'''
Screen 1
'''

class CustomTitleFilenameCombo(npyscreen.TitleFilenameCombo):
    def __init__(self, *args, **keywords):
        super(CustomTitleFilenameCombo, self).__init__(*args, **keywords)
        self.add_handlers({
            curses.ascii.SP:  self.Custom_h_change_value,
            curses.ascii.NL:  self.Custom_h_change_value,
            curses.ascii.CR:  self.Custom_h_change_value,
        })

    def Custom_h_change_value(self, *args, **keywords):
        F.write('success')
        self.h_change_value

class rdbForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        self.redisFile  =   self.add(npyscreen.TitleFilenameCombo, name = "RDB file to process:")
        self.redisFile.value_changed_callback = self.on_valueChanged
        self.vspace()

        rdbInfo = RDBOBJECT.get_rdb_infos()
        self.add(npyscreen.FixedText, value="RDB file information:", editable=False, color='LABEL')
        self.grid = self.add(npyscreen.GridColTitles, editable=False, column=3, max_height=3,
                col_titles=rdbInfo[0],
                values=rdbInfo[1])
                #values=[('123kB', '3', 21987)])

        self.vspace()
        self.chosenDb   =   self.add(npyscreen.TitleMultiSelect, max_height=15+3, max_width=30,
                name="Select DB Number from file:", 
                value   =   RDBOBJECT.get_seleced_db(), 
                values  =   RDBOBJECT.get_seleced_db())
        self.vspace()
        self.chosenServer=  self.add(npyscreen.TitleMultiSelect, max_height=10, rely=10, relx=self.chosenDb.width+10,
                name    =   "Select Redis server in which to inject:", 
                value   =   [], 
                values  =   RDBOBJECT.list_running_servers())
        self.vspace()

    def on_valueChanged(self, *args, **keywords):
        RDBOBJECT.add_filename(self.redisFile.value)
        rdbInfo = RDBOBJECT.get_rdb_infos()
        self.grid.values = rdbInfo[1]
        self.grid.display()

    def on_ok(self):
        selected = self.chosenServer.get_selected_objects()
        RDBOBJECT.add_target_redis_servers(selected)
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

