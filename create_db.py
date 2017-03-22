import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import transform_load as tl

def create_patients_table(psql_conn):
    create_sql = '''
                    CREATE TABLE patients (
                        patient_id VARCHAR(20) PRIMARY KEY,
                        diagnosis SMALLINT,
                        age SMALLINT,
                        gender VARCHAR(1),
                        education VARCHAR(20)
                    );
                '''
    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)',('patients',))
    if not cur.fetchone()[0]:
        cur.execute(create_sql)
    else:
        cur.close()
        return False

    psql_conn.commit()
    cur.close()
    return True

# extracts entrez_id -> index mapping using ROSMAP_RNASeq_entrez.csv
def create_entrez_id_to_index(in_file, delimiter, psql_conn):
    delimiter = tl.check_delimiter(delimiter)
    if delimiter is None:
        return False

    create_table = '''
                CREATE TABLE entrez_id_to_index (
                    entrez_id INTEGER PRIMARY KEY,
                    index INTEGER
                );
                '''.format(t=table)
    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)',('entrez_id_to_index',))
    if not cur.fetchone()[0]:
        cur.execute(create_table)
    else:
        cur.close()
        return False

    insert_sql = '''
                    INSERT INTO entrez_id_to_index (index, entrez_id)
                    VALUES (%s, %s);
                '''
    # should be the ROSMAP_RNASeq_entrez file
    with open(in_file) as fi:
        entrez_id_arr = fi.readline().strip().split(delimiter)
        index = 1
        for entrez_id in entrez_id_arr[2:]:
            cur.execute(insert_sql, (index, entrez_id))
            index += 1

    psql_conn.commit()
    cur.close()

    return True

def create_diagnosis_tables(psql_conn):
    tables = ['ad', 'nci', 'mci', 'na', 'other']
    create_table = '''
                CREATE TABLE {t} (
                    patient_id VARCHAR(20) PRIMARY KEY,
                    gene_expression double precision [16380]
                );
                '''
    cur = psql_conn.cursor()

    for diagnosis in tables:
        cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', (diagnosis,))
        if not cur.fetchone()[0]:
            cur.execute(create_table.format(t=diagnosis))
        else:
            cur.close()
            return False

    psql_conn.commit()
    cur.close()
    return True

def create_entrez_uniprot_table(psql_conn):
    #table_name = input('Enter the table name with entrez ID to uniprot ID mapping: ')
    create_table = '''
                CREATE TABLE entrez_uniprot (
                    entrez_id INTEGER PRIMARY KEY,
                    uniprot_id VARCHAR[],
                    gene_name VARCHAR(200)
                );
                '''

    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', ('entrez_uniprot',))
    if not cur.fetchone()[0]:
        cur.execute(create_table)
        psql_conn.commit()
        cur.close()
        return True

    else:
        cur.close()
        return False

def get_psql_db_info():
    info = {}
    info['user'] = input('PostgreSQL username: ')
    info['host'] = input('PostgreSQL host address: ')
    info['password'] = input('PostgreSQL database password: ')
    return info

# Creates PostgreSQL database
def psql_db_init(psql_conn):
    database_name = input('Enter a PostgreSQL database name: ')
    create_database = 'CREATE DATABASE {d};'.format(d=database_name)
    psql_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from pg_catalog.pg_database WHERE datname=%s)', (database_name,))

    if not cur.fetchone()[0]:
        cur.execute(create_database)
        psql_conn.commit()
        cur.close()
        psql_conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        print('{d} database created.'.format(d=database_name))
        return database_name
    else:
        cur.close()
        psql_conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        print('ERROR: database {d} already exists.'.format(d=database_name))
        ans = input('Do you want to use the current database {d} (y/n)?'.format(d=database_name))
        if ans:
            return database_name

    return False

# For first time connection. Creates databasee
def psql_init_connect(info):
    try:
        conn = psycopg2.connect(
                user=info['user'],
                host=info['host'],
                password=info['password']
            )
    except:
        return False
        print("ERROR: unable to connect to database.")

    database_name = ''
    while not database_name:
        database_name = psql_db_init(conn)
    info['database'] = database_name

    try:
        conn = psycopg2.connect(
                database=info['database'],
                user=info['user'],
                host=info['host'],
                password=info['password']
            )
    except:
        return False
        print("ERROR: unable to connect to database.")

    return conn

# For connection after database has been created
def psql_connect(info):
    try:
        conn = psycopg2.connect(
                database=info['database'],
                user=info['user'],
                host=info['host'],
                password=info['password']
            )
    except:
        return False
        print("ERROR: unable to connect to database.")

    return conn
