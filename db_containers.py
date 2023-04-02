import os
import sys
import socket
import time

import atexit
import uuid
from datetime import datetime
import pymysql

import pymongo
from pymongo import MongoClient

import redis

import cassandra.cluster
from cassandra.cluster import Cluster

class DbContainer:
    def __init__(self):
        self.name = None
        self.port = None
        self.cmd = None

    def _build_cmd(self):
        raise NotImplementedError

    def verify_connection(self):
        s = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM)
        try:
            s.connect(('127.0.0.1',self.port))
            print(
                f'{self.name} listening on 127.0.0.1:{self.port}')
            s.close()
            return True
        except:
            return False

    def db_init(self):
        self.verify_connection()

    def create(self):
        result = os.system(self.cmd)
        if (result == 0):
            print(f'Created {self.name}')
            self.db_init()

    def delete(self):
        cmd = f'docker stop {self.name}'
        result = os.system(cmd)
        if (result == 0):
            cmd = f'docker rm {self.name}'
            result = os.system(cmd)
            print(f'Removed {self.name}')

    def write(self):
        raise NotImplementedError

    def read(self):
        raise NotImplementedError

    def create_cursor(self):
        raise NotImplementedError

    def cleanup(self):
        pass


class MysqlContainer(DbContainer):
    def __init__(self, name, port, passwd=None, db_name='pluto'):
        self.name = name
        self.port = port
        self.passwd = passwd
        self.cmd = f'docker run -p {self.port}:3306 --name {self.name} -e MYSQL_ROOT_PASSWORD={self.passwd} -d mysql'
        self.db_name = db_name

    def _mysql_session(self, database=None):
        for i in range(6):
            try:
                cnx = pymysql.connect(user='root',
                                    password=self.passwd,
                                    host='127.0.0.1',
                                    port=self.port,
                                    database=database)
                return cnx
            except pymysql.err.OperationalError:
                print(f'Waiting for {self.name} database to start...')
                time.sleep(10)

        raise pymysql.err.OperationalError

    def db_init(self):
        self.verify_connection()

        cnx = self._mysql_session(None)
        cursor = cnx.cursor()

        query = (f"DROP DATABASE IF EXISTS `{self.db_name}`;")
        cursor.execute(query)

        query = (f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        cursor.execute(query)

        query = (f"USE {self.db_name}")
        cursor.execute(query)

        query = ('''
        CREATE TABLE posts(
            id VARCHAR(36),
            stamp VARCHAR(20)
        )
        ''')
        cursor.execute(query)

        cnx.commit()
        cursor.close()
        cnx.close()
        print(f'{self.name} initialized')

    def create_cursor(self):
        self.cnx = self._mysql_session(self.db_name)
        self.cursor = self.cnx.cursor()

    def cleanup(self):
        self.cursor.close()
        self.cnx.close()

    def _query(self, query):
        self.cursor.execute(query)
        self.cnx.commit()

    def write(self):
        id = str(uuid.uuid4())
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self._query(f'INSERT INTO `posts` VALUES("{id}","{time}")')

    def read(self):
        self._query("SELECT * FROM posts ORDER BY stamp DESC LIMIT 5;")

        stamps = []
        for row in self.cursor.fetchall():
            stamps.append(row[1])
        return stamps

    def delete_data(self):
        self._query("TRUNCATE posts")


class MongoContainer(DbContainer):
    def __init__(self, name, port, db_name='pluto'):
        self.name = name
        self.port = port
        self.db_name = db_name
        self.cmd = f'docker run -p {self.port}:27017 --name {self.name} -d mongo'

    def create_cursor(self):
        self.client = MongoClient(f'mongodb://localhost:{self.port}/')
        self.database = self.client[self.db_name]
        self.data = self.database.posts

    def write(self, data=[]):
        for stamp in data:
            item = {
                'stamp': stamp
            }
            self.database.posts.update_one(item, {'$set': item}, upsert=True)

    def read(self):
        stamps = []

        for post in self.database.posts.find().sort('stamp', pymongo.DESCENDING).limit(5):
            stamps.append(post['stamp'])
        return stamps

    def delete_data(self):
        self.database.posts.delete_many({})


class RedisContainer(DbContainer):
    def __init__(self, name, port):
        self.name = name
        self.port = port
        self.cmd = f'docker run -p {self.port}:6379 --name {self.name} -d redis'
    
    def create_cursor(self):
        self.cursor = redis.Redis(host='localhost', port=self.port, db=0)
    
    def write(self, data):
        self.cursor.mset({"LastInsertDate": f"{str(data[0])}"})

    def read(self):
        return self.cursor.get("LastInsertDate")

    def delete_data(self):
        self.cursor.delete('LastInsertDate')


class CassandraContainer(DbContainer):
    def __init__(self, name, port, keyspace='stamps'):
        self.name = name
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.cmd = f'docker run -p {self.port}:9042 --name {self.name} -d cassandra'

    def _cass_session(self, keyspace):
        for i in range(8):
            try:
                cluster = Cluster(['127.0.0.1'], port=self.port)
                session = cluster.connect(keyspace)
                return session
            except cassandra.cluster.NoHostAvailable:
                print(f'Waiting for {self.name} to start...')
                time.sleep(10)
        raise cassandra.cluster.NoHostAvailable

    def db_init(self):
        self.verify_connection()
        keyspace = None

        session = self._cass_session(keyspace)

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS stamps
            WITH REPLICATION = {'class':'SimpleStrategy','replication_factor' :1};
            """)

        session.set_keyspace(self.keyspace)
        session.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id text  PRIMARY KEY,
                stamp text
            );
            """)

        session.execute(
            f"insert into posts (id, stamp) values ('maxTimeStamp', '1975-01-01 00:00:00') IF NOT EXISTS")
        print(f'{self.name} initialized')

    def create_cursor(self):
        self.cursor = self._cass_session(self.keyspace)

    def write(self,stamps):
        sql = f"update posts set stamp = '{str(stamps[0])}' where id = 'maxTimeStamp' IF EXISTS"
        self.cursor.execute(sql)

    def read(self):
        result = self.cursor.execute(
            "select stamp from posts where id = 'maxTimeStamp'")
        return result[0].stamp

    def delete_data(self):
        self.cursor.execute("delete from posts where id = 'maxTimeStamp'")
        # Had to add this or write() would fail next time.
        self.cursor.execute(
            f"insert into posts (id, stamp) values ('maxTimeStamp', '1975-01-01 00:00:00') IF NOT EXISTS")
