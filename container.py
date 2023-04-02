import os
import sys

from db_containers import MysqlContainer, MongoContainer, RedisContainer, CassandraContainer

def create_databases(db_queue):
    for d in db_queue:
        d.create()
        d.create_cursor()

def delete_databases(db_queue):
    for d in db_queue:
        d.delete()

def clear_databases(db_queue):
    for d in db_queue:
        d.delete_data()
    print('Deleted data in all dbs!')

def init_cursor(db_queue):
    for d in db_queue:
        d.create_cursor()

def active_containers(db_queue):
    # Use verify_connection method to determine if container is running
    # and responding on expected port.

    running = []
    failed = []
    for d in db_queue:
        if d.verify_connection():
            running.append(d)
        else:
            failed.append(d)
    return running, failed


if __name__ == '__main__':
    # read input arguments
    argument = len(sys.argv)
    if (argument > 1):
        argument = sys.argv[1]
        db_list = sys.argv[2:]

    # Create list of container instances based on CLI arguments
    db_queue = []

    if 'mysql' in db_list:
        db_queue.append(MysqlContainer('mysql-test', 5600,
                        os.getenv("DATABASE_PASSWORD")))
    if 'mongo' in db_list:
        db_queue.append(MongoContainer('mongo-test', 1800))
    if 'redis' in db_list:
        db_queue.append(RedisContainer('redis-test', 2400))
    if 'cassandra' in db_list:
        db_queue.append(CassandraContainer('cassandra-test', 1000))
    if 'all' in db_list:
        db_queue.append(MysqlContainer('mysql-test', 5600,
                        os.getenv("DATABASE_PASSWORD")))
        db_queue.append(MongoContainer('mongo-test', 1800))
        db_queue.append(RedisContainer('redis-test', 2400))
        db_queue.append(CassandraContainer('cassandra-test', 1000))

    # Then create or delete all those container types in this list
    if (argument == '-create'):
        create_databases(db_queue)
    elif argument == '-delete':
        delete_databases(db_queue)
    else:
        print('''Please provide one of the following options:
            -create [db types]      Create a new database container
            -delete [db types]      Delete an existing database container

            Where [db types] can be a space separated list of any of the following:
                mysql
                mongo
                redis
                cassandra
                all
        ''')
    
