import requests
import pandas as pd
import psycopg2
import numpy as np
from datetime import date, timedelta, datetime
import hashlib
import sys
import time
import re

# ------------------------------------------ 
#       Вспомогательные процедуры
# ------------------------------------------

# инициализация соединения с базой
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        if log_lev >= 3: print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        print(f"The error {e} occurred")
    return connection

# запрос на создание базы
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logg("Query executed successfully")
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
    return

# запрос с ответом "ОК / не ОК"
# p=False - вообще без ответа
def execute_query(connection, query, p=True):
    connection.autocommit = True
    cursor = connection.cursor()    
    try:
        cursor.execute(query)
        if log_lev >= 3: 
            if p: logg(f'Query executed successfully {query[0:20]}...')
        result = True    
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
        result = False
    return result


# запрос с выводом данных в табличной форме
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            dd = pd.DataFrame(np.array(result))
            dd.columns = [desc[0] for desc in cursor.description]
        else:
            dd = pd.DataFrame([])
        return dd
    except psycopg2.OperationalError as e:
        logg(f"The error occurred: {e}")
    return 


# лекарство от зависших ссессий
def base_reset():
    connection.rollback()
    connection.autocommit = True
    #connection.close()
    return


def create_sql_table(name, data, prkey):
    """
    Процедура создания новой таблицы в SQL
    На входе имя таблицы, датафрейм и название колонки первичного ключа. 
    Пустая строка '', если надо создать таблицу без ключа 
    Команда создание табилцы формируется из названия колонок датафрейма и типа данных.
    В случает существования таблицы команда игнорируется.
    """
    cols = dict(data.dtypes)
    sql_st = """CREATE TABLE
    IF NOT EXISTS """ + name + """ (
        """
    for c in cols:
        dtyp = cols[c]
        if dtyp == 'int64': cols[c] = 'INTEGER'
        if dtyp == 'float64': cols[c] = 'NUMERIC'        
        if dtyp == 'datetime64[ns]': cols[c] = 'TIMESTAMP'
        if dtyp == 'object': cols[c] = 'VARCHAR'
        if dtyp == 'bool': cols[c] = 'VARCHAR'            
        if c == prkey: cols[c] = 'SERIAL PRIMARY KEY'

        sql_st = sql_st + c + " " + cols[c] + """, """
    sql_st = sql_st[:-2]
    sql_st = sql_st +  ")"
    execute_query(connection, sql_st)
    return


def export_to_sql(table, data, p=True):
    """
    Процедура вставки новых строк в SQL
    На входе имя таблицы и датафрейм
    Команда на добавление формируется из названия колонок датафрейма и его данных.
    Один запрос включает максимум 1000 команд.
    В случает конфликта команда на добавление строчки - игнорируется.
    """
    inrows = 1000
    for l in range(len(data) // inrows + 1):
        insert_row = """INSERT INTO """ + table  + """ (""" + ', '.join(data.columns) + """ ) VALUES"""
        cols = data.columns
        tt = data.iloc[l*inrows:l*inrows + inrows]
        for i in range(len(tt)):
            istr = '('
            for c in cols:
                if tt[c].dtypes != 'int' and tt[c].dtypes != 'float64':                    
                    nstr = str(tt.iloc[i][c]).replace("'", "")
                    if nstr == 'nan' or nstr == 'NaN' or nstr == 'None':
                        istr += "NULL, "
                    else:
                        istr += f"'{nstr}', "                   
                else:
                    nstr = str(tt.iloc[i][c])
                    if nstr == 'nan' or nstr == 'NaN' or nstr == 'None':
                        istr += "NULL, "
                    else:
                        istr += nstr + ', '
            istr = istr[:-2]
            istr += '), '+"\r\n"
            insert_row += istr
        insert_row = insert_row[:-4]
        insert_row += ' ON CONFLICT DO NOTHING ;'
        execute_query(connection, insert_row, False)
    
    if p:
        logg(f'Экспортировано в sql базу строк {l*inrows + len(tt)}', 3)  
    return


def generate_md5_hash(input_string):
    """
    Процедура расчета хэша входной строки по алгоритму md5
    Используется в таблицах, где нет одной уникальной колонки
    """
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()


def init_connection():
    """
    Процедура инициализации соединения с SQL базой
    Настройки соединения берутся из файла
    Из этого же файла берутся ключи аутентификации API
    """
    global connection
    global head
    global log_lev
    path = ''
    db_set = dict()
    file = open(path + "db_connect.txt")
    for line in file:
        k, v = line.strip().split('=')
        db_set[k] = v
    file.close()
    log_lev = int(db_set['log_level'])    
    connection = create_connection(db_set['dbname'], db_set['uname'], db_set['pass'], db_set['server'], db_set['port'])
    head={}
    head['mir_fandom'] = [db_set['oz_mirfandom_id'], db_set['oz_mirfandom']]
    head['omg'] = [db_set['oz_omg_id'], db_set['oz_omg']]
    head['fandom'] = [db_set['oz_fandom_id'], db_set['oz_fandom']]
    db_set = dict()
    return

def logg(st='', lev=0):  
    st = st.replace("'", "")
    st = st.replace('"', '')    
    if lev <= log_lev: print(f'{st}')
    sql = f"INSERT INTO log (date, script, mess, log_lev) VALUES ('{datetime.now()}', 'oz', '{st}', {lev})"
    execute_query(connection, sql, False)    
    return    

# ------------------------------------------ 
#              ELT скрипты
# ------------------------------------------




def etl_oz_all():
    """
    Обновление всех таблиц OZ за раз, с "одной кнопки"
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю обновление всех таблиц OZ -------------- ', 2)  

    try:
        etl_oz_products()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении товаров: {e}')

    try:
        etl_oz_postings()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении отправлений: {e}')

    try:
        etl_oz_stocks()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении остатков на складах FBO: {e}')

    try:
        etl_oz_returns()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении возвратов: {e}')

    try:
        etl_oz_prod_actions()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении товаров в акциях: {e}')

    try:
        elt_oz_discounts()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении запросов на скидку: {e}')

    try:
        elt_oz_transaction()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении транзакций: {e}')

    try:
        etl_oz_category()
    except Exception as e:
        logg(f'!!! Ошибка при обновлении категорий: {e}')

    try:
        etl_currency() 
    except Exception as e:
        logg(f'!!! Ошибка при обновлении курсов валют: {e}')       
        
    logg(f'------------ Обновление всех таблиц OZ завершено ------------ ', 2)
    return


# ------------------------------------------ 
#              Входная процедура
# ------------------------------------------

def start(event, context):
    """
    Корневая процедура скрипта
    Инициализация подключения к SQL и GS
    запуск по переданному параметру процедур:
        loaddash: elt_gs_load_dash() - загрузка дашбордов сотрудников
        load_stats_dash: elt_gs_load_stats_dash() - загрузка дашбордов статистики
        costprice: elt_gs_costprice() - загрузка себестоимости
        roadmap: etl_gs_roadmap() - загрузка дорожной карты 
        all: все процедуры сразу 
    """

    global log_lev
    log_lev = 0

    try:
        command = event['messages'][0]['details']['payload']
    except:
        command = event
    
    print(f'*** Старт скрипта sher_ozapi_to_sql с параметром {command} в {datetime.now()} ***') 
    
    command = command + '   '
    cc = command.split(' ')
    func = cc[0]
    fromdate = cc[1]

    #connection = None
    i = 0
    while i < 5:
        i +=1
        try:
            init_connection()
            base_reset()
            i = 10
        except:
            print(f'Попытка {i} установить соединение с базой SQL провалилась')
            time.sleep(3)

    try:
        connection
    except:
        print('Нет соединения с SQL базой!')
    else:
        logg(f'*** Команда для sher_ozapi_to_sql - [{command}] ***', 1)         
        match func:
            case 'all': etl_oz_all()
            case 'products': etl_oz_products()
            case 'postings': etl_oz_postings(fromdate)
            case 'stocks': etl_oz_stocks()
            case 'returns': etl_oz_returns(fromdate)
            case 'prod_actions': etl_oz_prod_actions()
            case 'discounts': elt_oz_discounts()  
            case 'transaction': elt_oz_transaction(fromdate)  
            case 'category': etl_oz_category()                                    
            case 'currency': etl_currency()
            case _: 
                logg('Не задан параметр!') 
        logg(f'*** Выполнена команда - [{command}] ***', 1) 
        connection.close()
    
         

    print(f'*** Завершение работы скрипта sher_ozapi_to_sql в {datetime.now()} ***') 


    return
