# rdb-to-redis-wrapper

A TUI interface to easily inject keys from RDB file into a running Redis server.

- Navigation is done using <ARROWS>
- Confirming is done using <ENTER>
- Selecting/Unselecting is done using <SPACE>

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
