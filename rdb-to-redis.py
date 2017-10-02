#!/usr/bin/env python3.5
# -*-coding:UTF-8 -*

import npyscreen, curses
import sys, os
import time, datetime
import argparse
import json
import redis

ALLREGEX = {}
REGMAXSIZE = 0

class rdbForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        self.redisFile  =   self.add(npyscreen.TitleFilenameCombo, name = "RDB file to process:")
        self.vspace()

        self.add(npyscreen.FixedText, value="RDB file information:", editable=False, color='LABEL')
        self.grid = self.add(npyscreen.GridColTitles, editable=False, column=3, max_height=3,
                col_titles=['Size', '# of DB', '# of Keys'],
                values=[('123kB', '3', 21987)])

        self.vspace()
        self.chosenDb   =   self.add(npyscreen.TitleMultiSelect, max_height=5, value = [], name="Select DB Number from file:", values = ['0', '1', '2', '3', '4'])
        self.vspace()
        self.chosenServer   =   self.add(npyscreen.TitleMultiSelect, max_height=5, value = [], name="Select Redis server in which to inject:", values=['*:8888', '*:6379', '*:6380'])
        self.vspace()


    def on_ok(self):
        selected = self.chosenServer.get_selected_objects()
        for sel in selected:
            ALLREGEX[sel] = []
        self.parentApp.switchForm('FILTER')


    def on_cancel(self):
        sys.exit(0)


class filterForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        self.regex  =   self.add(npyscreen.TitleText, name='Regex:', value='')
        self.vspace(2)
        self.add(npyscreen.TitleFixedText, name='Select redis database for which this regex applies:', value='', editable=False)
        self.tree   =   self.add(npyscreen.MLTreeMultiSelect)
        treedata    =   npyscreen.NPSTreeData(content='Redis servers', selectable=False, ignoreRoot=False)
        for serv in ALLREGEX.keys():
            n = treedata.newChild(content=serv, selectable=True)
            for r in ALLREGEX[serv]:
                n.newChild(content=r, selectable=False)

        self.tree.values = treedata

        self.add_button = self.add(npyscreen.ButtonPress, name = 'Add', relx = 20,rely= 25)
        self.add_button.whenPressed = self.addReg


    def addReg(self):
        selected = self.tree.get_selected_objects(return_node=False)
        regVal = self.regex.value
        for sel in selected:
            ALLREGEX[sel].append(regVal)
            global REGMAXSIZE
            REGMAXSIZE = REGMAXSIZE if REGMAXSIZE > len(regVal) else len(regVal)

        treedata    =   npyscreen.NPSTreeData(content='Redis servers', selectable=False, ignoreRoot=False)
        for serv in ALLREGEX.keys():
            n = treedata.newChild(content=serv, selectable=True)
            for r in ALLREGEX[serv]:
                n.newChild(content=r, selectable=False)

        self.tree.values = treedata
        self.regex.value = ''
        self.tree.display()

    def on_cancel(self):
        self.parentApp.switchForm('MAIN')

    def on_ok(self):
        self.parentApp.switchForm('CONFIRM')

class confirmForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        curY = 0
        self.rdbFile = self.add(npyscreen.BoxBasic, name='RDB file', max_width=20, relx=2, max_height=15, editable=False)
        self.vspace(2)
        curY += 17+2

        boxsize = REGMAXSIZE+8
        i=0
        for serv, reg in ALLREGEX.items():
            if boxsize < 40:
                box = self.add(npyscreen.BoxTitle, name=serv, max_width=boxsize, rely=curY, relx=2+i*boxsize, max_height=15, editable= False)
            else:
                box = self.add(npyscreen.BoxTitle, name=serv, max_width=boxsize, relx=2, max_height=10, editable= False)

            box.values = reg
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
    parser.add_argument('-r', '--redis', required=True, dest='redisPort', help='The redis port in which data must be copied')

    args = parser.parse_args()

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

