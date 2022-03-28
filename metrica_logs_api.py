from collections import namedtuple
import logs_api
import time
import utils
import sys
import requests
from datetime import datetime, timedelta
import logging
import ya_met_db as db
import yamet
import jsons


def setup_logging():
    global logger
    logger = logging.getLogger('logs_api')
    logging.basicConfig(stream=sys.stdout,
                        level='INFO',
                        format='%(asctime)s %(processName)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', )


def get_date_period(options):
    if options.mode is None:
        start_date_str = options.start_date
        end_date_str = options.end_date
    else:
        if options.mode == 'regular':
            start_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
                .strftime(utils.DATE_FORMAT)
            end_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
                .strftime(utils.DATE_FORMAT)
        elif options.mode == 'regular_early':
            start_date_str = (datetime.datetime.today() - datetime.timedelta(1)) \
                .strftime(utils.DATE_FORMAT)
            end_date_str = (datetime.datetime.today() - datetime.timedelta(1)) \
                .strftime(utils.DATE_FORMAT)
        elif options.mode == 'history':
            start_date_str = utils.get_counter_creation_date(
                config['counter_id'],
                config['token']
            )
            end_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
                .strftime(utils.DATE_FORMAT)
    return start_date_str, end_date_str


def build_user_request(config, source, start_date_str, end_date_str):
    '''options = utils.get_cli_options()
    logger.info('CLI Options: ' + str(options))
    start_date_str, end_date_str = get_date_period(options)
    source = options.source
    '''

    # Validate that fields are present in config
    assert '{source}_fields'.format(source=source) in config, \
        'Fields must be specified in config'
    fields = config['{source}_fields'.format(source=source)]

    # Creating data structure (immutable tuple) with initial user request
    UserRequest = namedtuple(
        "UserRequest",
        "token counter_id start_date_str end_date_str source fields"
    )

    user_request = UserRequest(
        token=config['token'],
        counter_id=config['counter_id'],
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        source=source,
        fields=tuple(fields),
    )

    logger.info(user_request)
    utils.validate_user_request(user_request)
    print(user_request)
    return user_request


def integrate_with_logs_api(config, user_request):
    for i in range(config['retries']):
        time.sleep(i * config['retries_delay'])
        try:
            # Creating API requests
            api_requests = logs_api.get_api_requests(user_request)

            for api_request in api_requests:
                logger.info('### CREATING TASK')
                logs_api.create_task(api_request)
                print(api_request)

                delay = 20
                while api_request.status != 'processed':
                    logger.info('### DELAY %d secs' % delay)
                    time.sleep(delay)
                    logger.info('### CHECKING STATUS')
                    api_request = logs_api.update_status(api_request)
                    logger.info('API Request status: ' + api_request.status)

                logger.info('### SAVING DATA')
                for part in range(api_request.size):
                    logger.info('Part #' + str(part))
                    logs_api.save_data(api_request, part)

                logger.info('### CLEANING DATA')
                logs_api.clean_data(api_request)
        except Exception as e:
            logger.critical('Iteration #{i} failed'.format(i=i + 1))
            logger.critical(e)
            if i == config['retries'] - 1:
                raise e


def get_start_date():
    try:
        sql_last_date = db.req_get_val('select dateTime FROM logs_api ORDER BY dateTime desc LIMIT 1')
        if len(sql_last_date) != 0:
            start_date_str = sql_last_date[0]["dateTime"]
        else:
            start_date_str = '2021-01-01'
    except Exception as ex:
        logging.info(f'Error: {ex}')
    finally:
        return start_date_str


def logs_api_table(start_date_str, end_date_str):
    try:
        # НАЧАЛЬНАЯ КОНФИГУРАЦИЯ
        config = utils.get_config()
        db.req_query_get_data('SET GLOBAL max_allowed_packet=80*1024*1024;')
        start_time = time.time()

        if start_date_str != (datetime.now() - timedelta(1)).strftime('%Y-%m-%d'):
            logger.info('Обновление таблицы Logs_API')
            user_request = build_user_request(
                config=config,
                source='visits',
                start_date_str=start_date_str,
                end_date_str=end_date_str
            )

            integrate_with_logs_api(config, user_request)
            
            end_time = time.time()
            logger.info('### TOTAL TIME: %d minutes %d seconds' % (
                (end_time - start_time) / 60,
                (end_time - start_time) % 60
            ))

            # ------
            # Logs_API ======================================================#
            yamet.logs_api = yamet.logs_api[0].split("\n")
            logs_api_db_data = []
            for i in yamet.logs_api:
                splitted = i.split("\t")
                logs_api_db_data.append({
                    'ClientID': splitted[0], 
                    'visitID': splitted[9],
                    'goalsID': splitted[3],
                    'tLastDirectPlatform': splitted[5], 
                    'domain': splitted[6],
                    'adDirect': splitted[4], 
                    'regionCity': splitted[8],
                    'deviceCategory': splitted[2],
                    'operatingSystemRoot': splitted[7],
                    'dateTime': splitted[1] 
                    })
            del logs_api_db_data[0]

            quote = "\u0027"
            query = f"INSERT INTO logs_api (ClientID, visitID, goalsID, tLastDirectPlatform, domain, adDirect, regionCity, deviceCategory, operatingSystemRoot, dateTime) VALUES "
            for c in logs_api_db_data:
                query += f"('{c['ClientID']}', '{c['visitID']}', '{c['goalsID']}', '{c['tLastDirectPlatform']}', '{c['domain']}', '{c['adDirect']}', '{c['regionCity'].replace(quote, '')}', '{c['deviceCategory']}', '{c['operatingSystemRoot']}', '{c['dateTime']}'),"
            query = query[:-1]
            db.req_query_get_data(query=query, sucs_msg='\nТаблица Logs_API успешно заполнена\n')
        else:
            logger.info('Обновление Logs_API не требуется')
    except Exception as ex:
        logging.critical(f'Error{ex}')
        main()


def api_v1_table(start_date_str, end_date_str):
     # Api_v1 ======================================================#
    direct_accs = db.req_get_val("SELECT * FROM ads_cab")
    db.req_query_get_data("TRUNCATE TABLE api_v1")
    
    part = 1
    for da in direct_accs:
        if part == 49:
            print()
        print(f"Часть {part}/{len(direct_accs)}")
        #url_sources_direct_platforms = f"https://api-metrika.yandex.net/stat/v1/data?limit=10000&date1=2021-01-01&date2=2022-02-07&ids=86274673&dimensions=ym:ad:<attribution>DirectPlatform&direct_client_logins=tagil-319546-bspx&metrics=ym:ad:<currency>AdCost,ym:ad:<currency>AdCostPerVisit,ym:ad:clicks,ym:ad:visits"
        url_sources_direct_platforms = f"https://api-metrika.yandex.net/stat/v1/data?limit=10000&date1={start_date_str}&date2={end_date_str}&ids=86274673&dimensions=ym:ad:<attribution>DirectOrder,ym:ad:<attribution>DirectPlatform&direct_client_logins={da['direct_client_logins']}&metrics=ym:ad:<currency>AdCost,ym:ad:<currency>AdCostPerVisit,ym:ad:clicks,ym:ad:visits"
        url_sources_direct_platforms = requests.get(url=url_sources_direct_platforms, headers={
            'Authorization': f"OAuth {da['token']}"}).json()
        try:
            query = f"INSERT INTO api_v1 (direct_id, domain_name, rubAdCost, rubAdCostPerVisit, clicks, visits) VALUES "
            for c in url_sources_direct_platforms['data']:
                query += f"({c['dimensions'][0]['direct_id'].replace('N-', '')}, '{c['dimensions'][1]['name']}', '{c['metrics'][0]}', '{c['metrics'][1]}', '{c['metrics'][2]}', '{c['metrics'][3]}'),"
            query = query[:-1]
        except Exception as ex:
            logging.critical(f'Ошибка{ex}')
        finally:
            try:
                if len(url_sources_direct_platforms['data']) == 0:
                    logging.critical(f"Данных нет {da['direct_client_logins']} ===> Data == {len(url_sources_direct_platforms['data'])} !!!")
                else:
                    db.req_query_get_data(query=query, sucs_msg=f"\nРек. кабинет {da['direct_client_logins']}\n")
            except Exception as ex:
                logging.critical(f"Проверьте верны ли данные аккаунта: {da['direct_client_logins']} - {da['token']} \n {url_sources_direct_platforms['code']}/{url_sources_direct_platforms['message']}")
        part += 1


def get_tables(start_date_str='2021-01-01', end_date_str=(datetime.now() - timedelta(1)).strftime('%Y-%m-%d')):
    print(f'''
            
        ##### ПОЛУЧЕНИЕ ДАННЫХ #####
        
        {start_date_str}  ---  {end_date_str}                 

    ''')

    if datetime.strptime(end_date_str, '%Y-%m-%d') < datetime.strptime(start_date_str, '%Y-%m-%d'):
        logging.critical('ERROR: ОШИБКА ДАТЫ')
        main()
        return

    time.sleep(2)
    logs_api_table(start_date_str=get_start_date(), end_date_str=end_date_str)
    api_v1_table(start_date_str=start_date_str, end_date_str=end_date_str)
    main()


def choose_(msg='ВЫ УВЕРЕНЫ?'):
    wipe_res = input(f'{msg} (y/n): ')
    if wipe_res == 'y':
        return True
    elif wipe_res == 'n':
        return False
    else:
        logging.info('Сброс')


def add_cabinet():
    try:
        cabinet_inf = input('Ввеедите (логин:токен): ').split(':')
        login = cabinet_inf[0]
        token = cabinet_inf[1]
    except Exception as ex:
        logging.critical(f'ОШИБКА ВВОДА: {ex}')
        main()

    try:
        query = f"INSERT INTO ads_cab (direct_client_logins, token) VALUES ('{login}', '{token}');"
        db.req_query_get_data(query=query)
    except Exception as ex:
        logging.critical(f'Кабинет не добавлен: {ex}')
        main()
    finally:
        logging.info(f'Кабинет {login} успешно добавлен')
        main()


def del_cabinet():
    try:
        ads_cab_list = db.req_get_val('SELECT * FROM ads_cab')

        if len(ads_cab_list) == 0:
            logging.info('ТАБЛИЦА ads_cab ПУСТА')
            main()

        print(f'''

        ### УДАЛИТЬ КАБИНЕТ ###

        ==================

        ''')

        for idx, ads in enumerate(ads_cab_list):
            print(f"        {idx}. {ads['direct_client_logins']}")

        print(f'''

        ==================

        1. Выбрать кабинет;
        2. Обратно в меню

        ''')

        menu_res = input('Пункт меню: ')

        if menu_res == '1':
            cab_num = input('Номер кабинета: ')
            if choose_() is True:
                try:
                    query = f"DELETE FROM ads_cab WHERE direct_client_logins = '{ads_cab_list[int(cab_num)]['direct_client_logins']}'"
                    db.req_query_get_data(query=query)
                except Exception as ex:
                    logging.critical(f'ОШИБКА УДАЛЕНИЯ: {ex}')
                    del_cabinet()
            else:
                del_cabinet()
        elif menu_res == '2':
            main()

    except Exception as ex:
        logging.critical(f'ОШИБКА: {ex}')
        del_cabinet()
    finally:
        logging.info(f'КАБИНЕТ УСПЕШНО УДАЛЕН')
        main()


def reload_base(wipe=False):
    try:
        query = [
                    "TRUNCATE TABLE logs_api;",
                    "TRUNCATE TABLE api_v1;",
                    "TRUNCATE TABLE ads_cab;"
        ]

        if choose_() is True:
            if wipe is True:
                 db.req_query_loop(queryes_list=query)
            else:
                db.req_query_loop(queryes_list=query[:-1])
        else:
            main()
    except Exception as ex:
        logging.critical(f'ОШИБКА ЧИСТКИ: {ex}')
        main()
    finally:
        main()


def main():
    setup_logging()
    print('''


        ### МЕНЮ ###

        ==================

        Список периодов:
        1. За вчера;
        2. За неделю;
        3. За месяц;
        4. За весь период
        5. Указать период

        ==================

        6. Добавить кабинет
        7. Удалить кабинет

        ==================

        8. Очистить Logs_api
        9. ПОЛНОСТЬЮ ОЧИСТИТЬ БАЗУ


    ''')
    menu_case = input('Пункт меню: ')

    if menu_case == '1':
        start_date_str = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        get_tables(start_date_str=start_date_str)
    elif menu_case == '2':
        start_date_str = (datetime.now() - timedelta(7)).strftime('%Y-%m-%d')
        get_tables(start_date_str=start_date_str)
    elif menu_case == '3':
        start_date_str = (datetime.now() - timedelta(31)).strftime('%Y-%m-%d')
        get_tables(start_date_str=start_date_str)
    elif menu_case == '4':
        get_tables()
    elif menu_case == '5':
        start_date_str = input('Начальная дата: ')
        end_date_str = input('Конечная дата: ')
        get_tables(start_date_str=start_date_str, end_date_str=end_date_str)
    elif menu_case == '6':
        add_cabinet()
    elif menu_case == '7':
        del_cabinet()
    elif menu_case == '8':
        reload_base()
    elif menu_case == '9':
        reload_base(wipe=True)
    else:
        logging.critical('ОШИБКА ВЫБОРА')
        main()

if __name__ == '__main__':
    # НАЧАЛО РАБОТЫ
    main()