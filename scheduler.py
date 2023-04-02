from threading import Timer
import time
import sys
import os
import atexit

from db_containers import MysqlContainer, MongoContainer, RedisContainer, CassandraContainer
from container import create_databases, clear_databases, delete_databases, init_cursor, active_containers

mysqldb = MysqlContainer('mysql-test', 5600,
                       os.getenv("DATABASE_PASSWORD"))
mongodb = MongoContainer('mongo-test', 1800)
redisdb = RedisContainer('redis-test', 2400)
cassandradb = CassandraContainer('cassandra-test', 1000)

db_queue = [mysqldb,mongodb,redisdb,cassandradb]

# -------------
# time loop
# -------------

def status(stamps, db):
    print(f'Data in {db}:')
    for stamp in stamps:
        print(stamp)
    time.sleep(2)


def mysql():
    mysqldb.write()


def mongo():
    stamps = mysqldb.read()
    status(stamps, 'mysql')
    mongodb.write(stamps)


def redis():
    stamps = mysqldb.read()
    redisdb.write(stamps)


def cassandra():
    stamps = mysqldb.read()
    cassandradb.write(stamps)


def verify():
    stamps = mongodb.read()
    status(stamps, 'mongo')
    lastInsertDate = redisdb.read()
    print(f'Data in Redis: LastInsertDate = {lastInsertDate.decode("utf-8")}')
    lastUpdateDate = cassandradb.read()
    print(f'Data in Cassandra: LastUpdateDate = {lastUpdateDate}')


def timeloop():
    print(f'--- LOOP: ' + time.ctime() + ' ---')
    mysql()
    mongo()
    redis()
    cassandra()
    verify()
    Timer(5, timeloop).start()


# Check which containers are already running and listening
# on expected port, and which appear to be dead/missing
active_list, restart_list = active_containers(db_queue)

# read input argument
argument = len(sys.argv)
if (argument > 1):
    argument = sys.argv[1]

# if -clear input argument, delete data
if (argument == '-clear'):
    init_cursor(active_list)
    clear_databases(active_list)
    sys.exit()

elif argument == '-run':
    # Only create those not responding (assumed dead)
    create_databases(restart_list)
    init_cursor(active_list)
    timeloop()

elif argument == '-shutdown':
    delete_databases(active_list)

else:
    print('''Please provide one of the following options:
            -clear      Delete all data from databases
            -run        Create and initialize containers (as needed) and run CDC loop
            -shutdown   Destroy all containers
        ''')

@atexit.register
def handler():
    active_list, restart_list = active_containers(db_queue)
    print('Running cleanup on each db object...')
    for d in active_list:
        d.cleanup()
