import  psycopg2 as pgsql
from    psycopg2 import OperationalError, pool
import  configparser
from    util.logger import log
from    contextlib import contextmanager


st_create_table = """
create table if not exists oracle.GFSS_JOURNAL
(
  id              SERIAL PRIMARY KEY,
  gfss_in_nom     VARCHAR(30),
  gfss_in_date    timestamp,
  gcvp_out_nom    VARCHAR(30),
  gcvp_out_date   timestamp,
  gfss_out_nom    VARCHAR(30),
  gfss_out_date   timestamp,
  dat_reg         timestamp,
  gcvp_order_nom  VARCHAR(30),
  gcvp_order_date timestamp,
  id_doc          VARCHAR(2000),
  delivery        boolean default False,
  date_delivery   timestamp default current_timestamp
)
"""

st_create_index_1 = 'create unique index if not exists pk_gfss_journal on oracle.gfss_journal(id)'
st_create_index_2 = 'create index if not exists xn_gfss_journal_delivery on oracle.gfss_journal(delivery)'

config = configparser.ConfigParser()
config.read('db_config.ini')

ps_config = config['postgre']

debug_level = ps_config['debug_level']

db_database = ps_config['database']
db_user = ps_config['db_user']
db_password = ps_config['db_password']
db_host = ps_config['db_host']
db_max_connection = ps_config['db_max_connection']
db_min_connection = ps_config['db_min_connection']

#log.info(f'PS_CONNECT. HOST: {db_host}, DATABASE: {db_database}, DB_USER: {db_user}, DB_PASSWORD: {db_password} ')

try:
    _pool = pgsql.pool.SimpleConnectionPool(db_min_connection, db_max_connection,
                                            database=db_database,
                                            user=db_user,
                                            password=db_password,
                                            host=db_host)
except OperationalError as error:
    log.error(f'Ошибка создания пула соединений к БД PostGreS {db_host}:{db_database} : {error}')

log.info(f'Пул соединенй БД PostGreS создан.')

@contextmanager
def get_cursor():
    conn = _pool.getconn()
    try:
        yield conn.cursor()
    finally:
        _pool.putconn(conn)


def get_connection():
    if _pool:
        # print('Получаем соединение из POOL')
        return _pool.getconn()
    else:
        return None

def ps_create_table():
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(st_create_table)
            cursor.execute(st_create_index_1)
            cursor.execute(st_create_index_2)
        conn.commit()
        conn.close()
        log.info(f'CREATE POSTGRES TABLE. SUCCESS')
    except Exception as e:
        log.error(f'CREATE POSTGRES TABLE. ERROR: {e}')
    finally:
        _pool.putconn(conn)


def select(stmt):
    sel_rec = []
    mistake = 0
    err_mess = ''
    conn = get_connection()
    with conn.cursor() as cursor:
        try:
            if int(debug_level) > 3:
                log.info(f"\nВыбираем данные: {stmt}")
            cursor.execute(stmt)
            sel_rec = cursor.fetchall()
        except Exception as e:
            err_mess = f"===== POSTGRESQL ERROR SELECT: {stmt}\n{e}" 
            mistake = 1
            log.error(err_mess)
        finally:
            _pool.putconn(conn)
            return mistake, sel_rec, err_mess


def set_delivered(stmt):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            if int(debug_level) > 3:
                log.info(f"SET DELIVERY. STMT: {stmt}")
            cursor.execute(stmt)
            cursor.execute('commit')
    except Exception as e2:
        log.error(f"\n<<------ ERROR ------>>")
        log.error(f"{e2}")
        log.error(stmt)
        log.error(f"<<------ ERROR ------>>\n")
    finally:
        _pool.putconn(conn)


if __name__ == "__main__":
    ps_create_table()
    records = select('select * from autodrom_result')
    for rec in records:
        log.info(f"-->{rec}")
