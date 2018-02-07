# Простой failover для PostgreSQL
Схема простая.  
Есть три сервера:  
* Master
* Slave
* pgbouncer + failover 

Необходимо запускать failover на стороне pgbouncer.

Для запуска:
python simple_pg_failover.py


