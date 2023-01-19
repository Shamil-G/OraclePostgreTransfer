import cx_Oracle
import  configparser
from    util.logger import log
from    contextlib import contextmanager

config = configparser.ConfigParser()
config.read('db_config.ini')


def ip_addr():
    if using.startswith('PROD'):
        return request.remote_addr
    else:
        return request.remote_addr


def init_session(connection, requestedTag_ignored):
    cursor = connection.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY HH24:MI:SS'")
    #log.debug("--------------> Executed: ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY HH24:MI'")
    cursor.close()

ora_config = config['oracle']

debug_level = ora_config['debug_level']

lib_dir = ora_config['lib_dir']
db_dsn = ora_config['dsn']
db_user = ora_config['db_user']
db_password = ora_config['db_password']
db_max_connection = ora_config['db_max_connection']
db_min_connection = ora_config['db_min_connection']
db_pool_incr = ora_config['db_pool_incr']
db_max_lifetime_session = ora_config['db_max_lifetime_session']
db_timeout = ora_config['db_timeout']
db_wait_timeout = ora_config['db_wait_timeout']
db_encoding = ora_config['db_encoding']

log.info(f'LIB_DIR: {lib_dir}')

#cx_Oracle.init_oracle_client(lib_dir=lib_dir)
cx_Oracle.init_oracle_client(lib_dir='C:\instantclient_21_3')
ora_pool = cx_Oracle.SessionPool(db_user, db_password, db_dsn,
                                timeout=int(db_timeout), wait_timeout=int(db_wait_timeout),
                                max_lifetime_session=int(db_max_lifetime_session),
                                encoding=db_encoding, 
                                min=int(db_min_connection), max=int(db_max_connection), increment=int(db_pool_incr),
                                threaded=True, 
                                sessionCallback=init_session)
#except:
#    log.info(f'Пул соединенй БД Oracle создан. Timeout: {db_timeout}, wait_timeout: {db_wait_timeout}, '
#             f'max_lifetime_session: {db_max_lifetime_session}, min: {db_min_connection}, max: {db_max_connection}')

log.info(f'Пул соединенй БД Oracle создан. Timeout: {db_timeout}, wait_timeout: {db_wait_timeout}, '
         f'max_lifetime_session: {db_max_lifetime_session}, min: {db_min_connection}, max: {db_max_connection}')


def get_connection():
    if ora_pool:
        if int(debug_level) > 3:
            log.debug("Получаем курсор!")
        return ora_pool.acquire()
    else:
        return None


def plsql_execute(cursor, f_name, cmd, args):
    mistake = 0
    try:
        #if int(debug_level) > 3:
        log.debug(f"STMT: {cmd}\nPARAMS: {args}")
        cursor.execute(cmd, args)
    except cx_Oracle.DatabaseError as e:
        mistake = 1
        error, = e.args
        log.error(f"------execute------> ERROR. {f_name}. args: {args}")
        log.error(f"Oracle error: {error.code} : {error.message}")
    finally:
        return mistake


def plsql_proc(cursor, f_name, proc_name, args):
    mistake = 0
    try:
        cursor.callproc(proc_name, args)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        # log.error(f"-----plsql-proc-----> ERROR. {f_name}. IP_Addr: {ip_addr()}, args: {args}")
        log.error(f"-----plsql-proc-----> ERROR. {f_name}. ARGS: {args}")
        log.error(f"Oracle error: {error.code} : {error.message}")
        mistake = 1
    return mistake


if __name__ == "__main__":
    None
    # create_table()
