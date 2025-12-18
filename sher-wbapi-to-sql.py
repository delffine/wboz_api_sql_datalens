import requests
import pandas as pd
import psycopg2
import numpy as np
from datetime import date, timedelta, datetime
import hashlib
import sys
import time


# ------------------------------------------ 
#       Вспомогательные процедуры
# ------------------------------------------


#инициализация соединения с базой
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
        print(f"Create connection error: {e}")
    return connection

#запрос с ответом "ОК / не ОК"
#p=False - вообще без ответа
def execute_query(connection, query, p=True):
    connection.autocommit = True
    cursor = connection.cursor()    
    try:
        cursor.execute(query)
        if log_lev >= 3: 
            if p: logg(f'Query executed successfully {query[0:20]}...')
        result = True    
    except psycopg2.OperationalError as e:
        logg(f'Execute query error: {e}')
        result = False
    return result

#запрос с выводом данных в табличной форме
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
        logg(f"Read query error: {e}")
    return 

#лекарство от зависших ссессий
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
                    istr += "'" + str(tt.iloc[i][c]).replace("'", "") + "', "
                else:
                    nstr = str(tt.iloc[i][c])
                    if nstr == 'nan':
                        istr += "'NaN', "
                    else:
                        istr += nstr + ', '
                    #istr += str(tt.iloc[i][c]) + ', '
            istr = istr[:-2]
            istr += '), '+"\r\n"
            insert_row += istr
        insert_row = insert_row[:-4]
        insert_row += ' ON CONFLICT DO NOTHING ;'
        execute_query(connection, insert_row, False) 
        #if (len(data) > 1000) and (l % 5) == 0: 
        #    logg(f'Экспортировано в sql базу строк {l*inrows + len(tt)}', 3)
    if p:
        logg(f'Всего экспортировано в sql базу строк {l*inrows + len(tt)}', 3)    
    return

def generate_md5_hash(input_string):
    """
    Процедура расчета хэша входной строки по алгоритму md5
    Используется в таблицах, где нет одной уникальной колонки
    """
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()    
    
def get_wb(url, head, params):
    """
    Процедура запроса WB по API
    На входе адрес запроса, ключи аутентификации и параметры
    Возвращает ответный датафрейм, в названия колонок приведены к нижнему регистру
    и добавлена колонка партнера. 
    Ответный датафрейм, скорее всего, потребудет дополнительной обработки
    в зависимости от запроса.    
    """
    res = pd.DataFrame([])
    logg(f'----- Запрос WB по url /{url.split("/")[-1]}, с параметрами {params} -----', 3)
    #перебираем партнеров
    for h in head:
        headers = {"Authorization": head[h]}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dd = pd.DataFrame(response.json())
            #добавляем в табилцу партнера
            dd['partner'] = h
            #чтобы не брать пустые API ответы
            if len(dd) > 0:
                res = pd.concat([res, dd], ignore_index = True)
            logg(f'Получил {len(dd)} строк данных по {h}', 3)
        else:
            logg(f'Ошибка получения данных по API')
            logg(response.json())
    #приводим колонки к нижему регистру, чтобы все было однообразно
    res.columns = map(str.lower, res.columns)
    return res

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

    head={}
    head['basir'] = db_set['head_b']
    head['artur'] = db_set['head_a']
    head['kseniya'] = db_set['head_k']

    connection = create_connection(db_set['dbname'], db_set['uname'], db_set['pass'], db_set['server'], db_set['port'])
        
    db_set = dict()
    return

def logg(st='', lev=0):
    st = st.replace("'", "")
    if lev <= log_lev: print(f'{st}')
    sql = f"""INSERT INTO log (date, script, mess, log_lev) VALUES ('{datetime.now()}', 'wb', '{st}', {lev})"""
    execute_query(connection, sql, False)
    return

# ------------------------------------------ 
#              ELT скрипты
# ------------------------------------------



def etl_wb_all():
    """
    Обновление всех таблиц WB за раз, с "одной кнопки"
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю обновление всех таблиц WB -------------- ', 2)
    
    try:
        etl_wb_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении заказов: {e}')
    try:    
        etl_wb_sales()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении выкупов: {e}')
        
    try:
        etl_wb_income()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении поставок: {e}')

    try:
        etl_wb_stock()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении остатков на скаладах: {e}')
        
    try:
        etl_fbs_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении сборочных заданий: {e}')
        
    try:
        etl_wb_cards()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении карточек товаров: {e}')
        
    try:
        etl_wb_goodsreturn()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновленииь возвратов: {e}')
    
    try:
        etl_wb_adv_expenses()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении затрат на рекламу: {e}')
        
    try:
        elt_wb_adv_stat()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении статистики рекламы: {e}')

    try:
        etl_wb_feedback()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении отзывов: {e}')

    #Убираю воронку, потому что надо обновлять данные за месяц, а то и больше, и это занимает порядка 10 минут
    #try:
    #    etl_wb_voronka()
    #except Exception as e:
    #    logg(f'!!!!!! Ошибка при обновлении воронки продаж: {e}')
            
    try:
        etl_wb_finreport()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении финансового отчета: {e}')        
    
    logg(f'------------ Обновление всех таблиц WB завершено ------------ ', 2)
    return


def etl_wb_everyday():
    """
    Обновление таблиц WB, в которых данные меняются часто:
    все таблицы, кроме карточек товаров, финотчета.
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю ежедневное обновление таблиц WB -------------- ', 2)
    
    try:
        etl_wb_cards()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении карточек товаров: {e}')
            
    try:
        etl_wb_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении заказов: {e}')
    try:    
        etl_wb_sales()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении выкупов: {e}')
        
    try:
        etl_wb_income()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении поставок: {e}')

    try:
        etl_wb_stock()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении остатков на скаладах: {e}')
        
    try:
        etl_fbs_orders()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении сборочных заданий: {e}')
      
    try:
        etl_wb_goodsreturn()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновленииь возвратов: {e}')
    
    try:
        etl_wb_adv_expenses()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении затрат на рекламу: {e}')
        
    try:
        elt_wb_adv_stat()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении статистики рекламы: {e}')

    try:
        etl_wb_feedback()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении отзывов: {e}')

    #Убираю воронку, потому что надо обновлять данные за месяц, а то и больше, и это занимает порядка 10 минут
    #try:
    #    etl_wb_voronka()
    #except Exception as e:
    #    logg(f'!!!!!! Ошибка при обновлении воронки продаж: {e}')


    logg(f'------------ Ежедневное Обновление таблиц WB завершено ------------ ', 2)
    return


def etl_wb_everyweek():
    """
    Обновление таблиц WB, в которых данные меняются не часто: карточки товаров, финотчет
    Последовательно выполняются процедуры, описанные выше
    """  
    
    logg(f'------------- Начинаю еженедельное обновление таблиц WB -------------- ', 2)

    try:
        etl_wb_finreport()
    except Exception as e:
        logg(f'!!!!!! Ошибка при обновлении финансового отчета: {e}')        
    
    logg(f'------------ Еженедельное обновление таблиц WB завершено ------------ ', 2)
    return



def start(event, context):
    """
    Корневая процедура скрипта
    Инициализация подключения к SQL
    Запуск по переданному параметру процедур опроса API WB
    """
    global log_lev
    log_lev = 0  
     
    try:
        command = event['messages'][0]['details']['payload']
    except:
        command = event
    
    print(f'***{datetime.now()} - Старт скрипта sher_wbapi_to_sql с параметром {command}***') 

    command = command + '   '
    cc = command.split(' ')
    func = cc[0]
    fromdate = cc[1]
    
    i = 0
    while i < 5:
        i +=1
        try:
            init_connection()
            base_reset()
            i = 10
        except Exception as e:
            print(f'Попытка {i} установить соединение с базой SQL провалилась {e}')
            time.sleep(3)

    #try:
    #    connection
        #if connection.closed == 0:

    try:
        connection
    except Exception as e:
        print(f'Нет соединения с SQL базой! {e}')
    else:    
        logg(f'*** Команда для sher_wbapi_to_sql - [{command}] ***', 1) 
        match func:
            case 'all': etl_wb_all()
            case 'everyday': etl_wb_everyday()
            case 'everyweek': etl_wb_everyweek()
            case 'orders': etl_wb_orders(fromdate)
            case 'sales': etl_wb_sales(fromdate)
            case 'income': etl_wb_income(fromdate)
            case 'stock':  etl_wb_stock(fromdate)
            case 'fbs_orders':  etl_fbs_orders()
            case 'goodsreturn':  etl_wb_goodsreturn(fromdate)
            case 'adv_expenses':  etl_wb_adv_expenses(fromdate)
            case 'adv_stat':  elt_wb_adv_stat(fromdate)
            case 'finreport':  etl_wb_finreport()
            case 'cards':  etl_wb_cards()
            case 'stocks_coords': wb_stocks_coords()
            case 'feedback': etl_wb_feedback()
            case 'voronka': etl_wb_voronka(fromdate)
            case _: 
                logg('Не задан параметр!') 
        logg(f'***** Выполнена команда - [{command}] *****', 1) 
        connection.close()

    print(f'***{datetime.now()} - Завершение работы скрипта sher_wbapi_to_sql***') 


    return
