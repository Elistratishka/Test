import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
from rate import get_usd_rate


load_dotenv()


def create_bd():
    try:
        connection = psycopg2.connect(user=os.getenv("USER"),
                                      password=os.getenv("PASSWORD"),
                                      host=os.getenv("HOST"),
                                      port=os.getenv("PORT"),
                                      database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        create_table_query = '''CREATE TABLE test (
            id serial NOT NULL, 
            order_id INTEGER PRIMARY KEY,  
            price_usd INTEGER NOT NULL, 
            price_rub REAL NOT NULL, 
            order_time varchar(30) NOT NULL);'''
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица успешно создана в PostgreSQL")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_data(values: tuple):
    try:
        connection = psycopg2.connect(user=os.getenv("USER"),
                                      password=os.getenv("PASSWORD"),
                                      host=os.getenv("HOST"),
                                      port=os.getenv("PORT"),
                                      database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        for item in values:
            insert_query = ''' INSERT INTO test (id, order_id, price_usd, price_rub, order_time)
                VALUES (%s, %s, %s, %s, %s)'''
            item_tuple = (item[0],
                          item[1],
                          item[2],
                          float(item[2])*get_usd_rate(),
                          item[3])
            cursor.execute(insert_query, item_tuple)
            connection.commit()
        return print("Таблица успешно перенесена в БД")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_data() -> tuple:
    try:
        connection = psycopg2.connect(user=os.getenv("USER"),
                                      password=os.getenv("PASSWORD"),
                                      host=os.getenv("HOST"),
                                      port=os.getenv("PORT"),
                                      database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        cursor.execute("SELECT id, order_id, price_usd, order_time from test")
        return cursor.fetchall()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def change_data(comand: str, query):
    try:
        connection = psycopg2.connect(user=os.getenv("USER"),
                                      password=os.getenv("PASSWORD"),
                                      host=os.getenv("HOST"),
                                      port=os.getenv("PORT"),
                                      database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        if comand == 'DELETE':
            for item in query:
                delete_query = f'''DELETE from test where order_id = {item}'''
                cursor.execute(delete_query)
                connection.commit()
            print("Строки удалены из БД")
        if comand == 'INSERT':
            for item in query:
                insert_query = ''' INSERT INTO test (id, order_id, price_usd, price_rub, order_time)
                    VALUES (%s, %s, %s, %s, %s)'''
                item_tuple = (item[0],
                              item[1],
                              item[2],
                              float(item[2]) * get_usd_rate(),
                              item[3])
                cursor.execute(insert_query, item_tuple)
                connection.commit()
            print("Строки добавлены в БД")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def update_data(change: list):
    try:
        connection = psycopg2.connect(user=os.getenv("USER"),
                                      password=os.getenv("PASSWORD"),
                                      host=os.getenv("HOST"),
                                      port=os.getenv("PORT"),
                                      database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        if change:
            for item in change:
                update_query = f''' UPDATE test set id = %s, price_usd = %s, 
                price_rub = %s, order_time = %s 
                where order_id = %s'''
                item_tuple = (item[0],
                              item[2],
                              float(item[2])*get_usd_rate(),
                              item[3],
                              item[1])
                cursor.execute(update_query, item_tuple)
                connection.commit()
        print("В базу данных внесены изменения")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
