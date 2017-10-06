#!/usr/bin/env python3
# -*-coding:UTF-8 -*

import argparse
import os

import rdb_to_redis_tui
from rdb_to_redis_injector import inject



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read rdb file and copy wanted content into a running redis server.')
    parser.add_argument('-f', '--filename', required=False, dest='rdbFile', help='The RDB file from which data must be copied')
    parser.add_argument('-d', '--db', required=False, type=str, default='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15', dest='redisDb', help='The RDB databases number in which data must be taken')
    parser.add_argument('-s', '--serverRedis', required=False, type=str, dest='redisServer', help='The redis server in which data must be copied')
    parser.add_argument('-r', '--regex', required=False, type=str, dest='regex', help='The regex to be applied on the key, disable the TUI')

    args = parser.parse_args()

    RDBOBJECT = rdb_to_redis_tui.RDBObject()
    if args.rdbFile:
        RDBOBJECT.add_filename(os.path.realpath(args.rdbFile))
    
    if args.redisServer:
        args.redisServer = [item for item in args.redisServer.split(',')]
        RDBOBJECT.add_target_redis_servers(args.redisServer)
    
    if args.redisDb:
        args.redisDb = [int(item) for item in args.redisDb.split(',')]
        int_db = [int(db) for db in args.redisDb]
        RDBOBJECT.add_selected_db(int_db)

    if args.regex: #regex disable TUI
        server_with_re = {}
        server_with_type = {}
        for serv in args.redisServer:
           server_with_re[serv] = args.regex
           server_with_type[serv] = ['STRING', 'SET', 'ZSET', 'HSET', 'LIST', 'GEOSET', 'HYPERLOGLOG']

        inject(
        args.rdbFile,
        server_with_re,
        server_with_type,
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        True)
    else:

        rdb_to_redis_tui.RDBOBJECT = RDBOBJECT

        App = rdb_to_redis_tui.MyApplication()
        App.run()
        RDBOBJECT = rdb_to_redis_tui.RDBOBJECT
        inject(RDBOBJECT.filename, RDBOBJECT.get_servers_with_regex(), RDBOBJECT.get_selected_type(), RDBOBJECT.get_selected_db(), True)

