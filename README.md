# rdb-to-redis-wrapper

A TUI interface to easily inject keys from a RDB file into a running Redis server.

## Dependencies
- npyscreen 
- rdbtools
- virtualenv

## Installation

Run the installation script:
```
sudo sh install_dependencies.sh
```

## Navigation

- Navigation is done using ```<ARROWS>```
- Confirming is done using ```<ENTER>```
- Selecting/Unselecting is done using ```<SPACE>```


## Commandline

```
optional arguments:

  -h, --help            show this help message and exit
  -f RDBFILE, --filename RDBFILE
                        The RDB file from which data must be copied
  -d REDISDB, --db REDISDB
                        The RDB databases number in which data must be taken
  -s REDISSERVER, --serverRedis REDISSERVER
                        The redis server in which data must be copied
  -r REGEX, --regex REGEX
                        The regex to be applied on the key, disable the TUI
```
