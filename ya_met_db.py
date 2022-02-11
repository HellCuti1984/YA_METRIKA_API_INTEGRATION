from asyncio import QueueEmpty
import utils
import logging
import pymysql
import sqlite3
import time

def con_sqlite3(q):
    con = sqlite3.connect('example.db')
    exec_con = con.cursor()
    exec_con.execute(q)
    con.commit()
    con.close()


def connect_db():
    try:
        '''
        conn = pymysql.connect(
            host=config['host'],
            port=int(config['port']),
            user=config['user'],
            passwd=config['password'],
            database=config['database'],
            cursorclass=pymysql.cursors.DictCursor
        )'''
        config = utils.get_config()['mysql_db']
        conn = pymysql.connect(
            host=config['host'],
            port=int(config['port']),
            user=config['user'],
            passwd=config['password'],
            database=config['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
    except Exception as ex:
        logging.info(f"Error{ex}")
        time.sleep(10)
    finally:
        logging.info('Соединение с БД установлено')


def req_query_get_data(query, sucs_msg='Успех'):
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
    except Exception as ex:
        logging.info(f"Error{ex}")
        time.sleep(10)
    finally:
        logging.info(sucs_msg)


def req_query_loop(queryes_list):
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            for q in queryes_list:
                cursor.execute(q)
        conn.commit()
    except Exception as ex:
        logging.info(f"Error{ex}")
        time.sleep(10)
    finally:
        logging.info(f'Запросы выполнены ({len(queryes_list)} шт.)')
        conn.close()


def req_get_val(sql):
    try:
        sql_val = 0
        conn = connect_db()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            sql_val = cursor.fetchall()
    except Exception as ex:
        logging.info(f"Error{ex}")
        time.sleep(10)
    finally:
        logging.info('Запросы выполнены')
        conn.close()
        return sql_val
