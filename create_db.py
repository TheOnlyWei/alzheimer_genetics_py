import psycopg2
from pymongo import MongoClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

def get_mongodb_collection(database, collection, mongo_client):
    mongo_db = mongo_client[database]
    result = mongo_db[collection]
    return result

def entrez_uniprot_init(mongo_db):
    collection = input('Enter Entrez ID, uniprot ID, and gene name collection name: ')
    collection = mongo_db[collection]
    return collection

def create_diagnosis_tables(psql_conn, tables = []):
    create_table = '''
                CREATE TABLE {t} (
                    patient_id VARCHAR[20] PRIMARY KEY,
                    gene_expression double precision []
                );
                '''
    cur = psql_conn.cursor()

    for diagnosis in tables:
        cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', (diagnosis,))
        if not cur.fetchone()[0]:
            cur.execute(create_table.format(t=diagnosis))
        else:
            print('ERROR: table {t} already exists.'.format(t=diagnosis))

    psql_conn.commit()
    cur.close()

def create_entrez_uniprot_table(table, psql_conn):
    print('The Entre ID, Uniprot ID, and gene info. table will be named "{t}".'.format(t=table))
    #table_name = input('Enter the table name with entrez ID to uniprot ID mapping: ')

    create_table = '''
                CREATE TABLE entrez_uniprot (
                    entrez_id INTEGER PRIMARY KEY,
                    uniprot_id VARCHAR[],
                    gene_name VARCHAR(200)
                );
                '''

    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', (table,))
    if not cur.fetchone()[0]:
        cur.execute(create_table)
        psql_conn.commit()
        cur.close()
        print('"entrez_uniprot" table created.')
        return True

    else:
        cur.close()
        print('ERROR: table "entrez_uniprot" already exists.')
        print('Skipping "entrez_uniprot" table creation')
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
