import psycopg2
from pymongo import MongoClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

def mongo_db_init(mongo_client):
    database = input('Enter the name of your MongoDB database: ')
    db = mongo_client[database]
    return db

def gene_expr_collection_init(mongo_db):
    collection = input('Enter patient gene expression profile collection name: ')
    collection = mongo_db[collection]
    return collection

def patients_collection_init(mongo_db):
    collection = input('Enter patient age, gender, education collection name: ')
    collection = mongo_db[collection]
    return collection

def entrez_uniprot_init(mongo_db):
    collection = input('Enter Entrez ID, uniprot ID, and gene name collection name: ')
    collection = mongo_db[collection]
    return collection

# don't need this function
def create_entrez_id_to_index(in_file, delimiter, psql_conn):
    cur = psql_conn.cursor()
    create_table = '''
                CREATE TABLE entrez_id_to_index (
                    index INTEGER,
                    entrez_id INTEGER PRIMARY KEY
                );
                '''
    cur.execute(create_table)

    insert_table = '''
                INSERT INTO entrez_id_to_index (index, entrez_id)
                VALUES (%s, %s);
                '''
    # should be the ROSMAP_RNASeq_entrez file
    with open(in_file) as fi:
        entrez_id_arr = fi.readline().strip().split(delimiter)
        index = 0
        for entrez_id in entrez_id_arr[2:]:
            cur.execute(insert_table, (index, entrez_id))
            index += 1

    #psql_conn.commit()
    cur.close()

# Running statistic table for storing mean and population standard deviation
# as well as size of data for fast query.
def create_running_stat_table(psql_conn):
    print('Creating auxiliary tables "NCI", "MCI", "AD", "other", and "NA"...')
    diagnosis = ['NCI','MCI','AD','other','NA']
    create_table = '''
                    CREATE TABLE {t} (
                        entrez_id INTEGER PRIMARY KEY,
                        size INTEGER,
                        mean DOUBLE PRECISION,
                        std_pop DOUBLE PRECISION
                    );
                '''
    cur = psql_conn.cursor()
    for table in diagnosis:
        cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', (table,))
        if not cur.fetchone()[0]:
            cur.execute(create_table.format(t=table))
            print('{t} table created.'.format(t=table))
        else:
            cur.close()
            print('ERROR: table {t} already exists.'.format(t=table))
            return False

    #psql_conn.commit()
    cur.close()
    return True

def create_entrez_uniprot_table(psql_conn):
    print('The Entre ID, Uniprot ID, and gene info. table will be named "entrez_uniprot".')
    #table_name = input('Enter the table name with entrez ID to uniprot ID mapping: ')

    create_table = '''
                CREATE TABLE entrez_uniprot (
                    entrez_id INTEGER,
                    uniprot_id VARCHAR(20) PRIMARY KEY,
                    gene_name VARCHAR(200)
                );
                '''

    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', ('entrez_uniprot',))
    if not cur.fetchone()[0]:
        cur.execute(create_table)
        #psql_conn.commit()
        cur.close()
        print('"entrez_uniprot" table created.')
        return True

    else:
        cur.close()
        print('ERROR: table "entrez_uniprot" already exists.')
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
        #psql_conn.commit()
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
