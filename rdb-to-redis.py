#!/usr/bin/env python3.5
# -*-coding:UTF-8 -*

import npyscreen, curses
import sys, os
import time, datetime
import argparse
import json
import redis

class rdbForm(npyscreen.Form):
    def beforeEditing(self):
        sys.exit(0)

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

        #s  = self.add(npyscreen.TitleSliderPercent, out_of=100, value=35, name="Progress:", editable=False)


        # This lets the user interact with the Form.
        self.edit()



class confirmForm(npyscreen.ActionForm):
    def vspace(self, sp=1):
        self.nextrely += sp

    def create(self):
        curY = 0
        self.rdbFile = self.add(npyscreen.BoxBasic, name='RDB file', max_width=20, relx=2, max_height=15, editable=False)
        self.vspace(2)
        curY += 17+2
        self.box1 = self.add(npyscreen.BoxTitle, name='*:8888', max_width=20, relx=2, max_height=15, editable=False)
        self.box2 = self.add(npyscreen.BoxTitle, name='*:6379', max_width=20, rely=curY, relx=22, max_height=15, editable= False,
                contained_widget_arguments={
                    })
        curY += 17
        self.box2.values = ["regex1", "regex2"]

        self.edit()


class MyApplication(npyscreen.NPSAppManaged):
    def onStart(self):
        self.addForm('MAIN', rdbForm, name='RDB to Redis Server')
        self.addForm('filter', confirmForm, name='Confirm')
        #self.addForm('MAIN', confirmForm, name='Filtering')

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


