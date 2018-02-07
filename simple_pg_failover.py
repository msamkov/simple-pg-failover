#!/usr/bin/python
# -*- coding: utf-8 -*-

# настройки для проверки master
keepalivesIdle = 60 # интервал между проверками мастера
keepalivesCount = 3  # количество проверок перед failover
keepalivesInterval = 5 # интервал между проверками после первого отказа соединения на мастер

# настройки соединения master для БД
host='master.host'
port = 5432
dbname='demo1' 
user='postgres' 
connectTimeout=3

# настройки соединения slave по ssh
sshHost='slave.host'
sshUser='postgres'
sshPort = 22
promotePath = '/var/lib/postgresql/10/main/promote_on'

# после failover создаём файлик simple_pg_failover.guard для защиты от повторного выполнения failover
guardPath = '/etc/simple-pg-failover/simple_pg_failover.guard'
logPath   = '/etc/simple-pg-failover/simple_pg_failover.log'

# настройки для pgbouncer
pgbouncerSlavePath = '/etc/pgbouncer/pgbouncer.slave'
pgbouncerPath = '/etc/pgbouncer/pgbouncer.ini'

import psycopg2
import time
import sys
import os
import subprocess
import paramiko
import datetime
import shutil
import warnings


class SimplePgFailover:
    def __init__(self, keepalivesIdle, keepalivesCount, keepalivesInterval):
        self.keepalivesIdle     = keepalivesIdle
        self.keepalivesCount    = keepalivesCount
        self.keepalivesInterval = keepalivesInterval
        self.currKeepalivesCount = 0
        self.currKeepalivesIdle = keepalivesIdle
    def log(self, text):
        logFile = open(logPath, "a")
        logFile.write(str(datetime.datetime.now()) + ' ' + text + '\n')
        logFile.close()

    def start(self):
        # проверим если создан файл ограничитель, SimplePgFailover прекращает свою работу
        if self.checkGuard():
            sys.exit(0)

        while True:
            if self.currKeepalivesCount >= self.keepalivesCount:
                self.promoteSlave()

            self.checkMaster()

            if(self.currKeepalivesCount > 0):
                self.currKeepalivesIdle = self.keepalivesInterval
            else:
                self.currKeepalivesIdle = keepalivesIdle
            
            time.sleep(self.currKeepalivesIdle)

    # функция для проверки master
    def checkMaster(self):
        con = None
        try:
            connectionString = "host='{}' port={} dbname='{}' user='{}' connect_timeout={}".format(host, port, dbname, user, connectTimeout )
            con = psycopg2.connect(connectionString)
            self.currKeepalivesCount = 0
            self.log('Master {} online'.format(host))
        except psycopg2.DatabaseError as e:
            if con:
                con.rollback()
            self.currKeepalivesCount +=1
            self.log('Master {} offline'.format(host))
        finally:
            if con:
                con.close()

    # failover slave
    def promoteSlave(self):
        # по ssh влючить slave в обычный режим
        warnings.filterwarnings("ignore")
    client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=sshHost, username=sshUser, port=sshPort)
        client.exec_command('touch ' + promotePath)
        client.close()
    warnings.filterwarnings("always")

        #в pgbouncer сменить host с master на slave
        shutil.copy(pgbouncerSlavePath, pgbouncerPath)
        #перезагрузим pgbouncer
        subprocess.call("service pgbouncer reload", shell=True)
        self.log('Promote {} slave'.format(host))
        self.createGuard()

    # добавить файл для защиты от повторного выполнения failover
    def createGuard(self):
        filePath = open(guardPath, "w")
        filePath.close()
        sys.exit(0)

    # преверить наличия файла для защиты от повторного выполнения failover
    def checkGuard(self):
        if os.access(guardPath, os.F_OK) == True:
           return True
        else:
            return False

if __name__ == '__main__':
    simplePgFailover = SimplePgFailover(keepalivesIdle, keepalivesCount, keepalivesInterval)
    simplePgFailover.start()
