import create_db as cdb

# psql_conn is the PostgreSQL database connection
def get_gene_stat(entrez_id, diagnosis, column, psql_conn):
    if column == 'std':
        column = 'std_pop'
    if not psql_conn:
        info = cdb.get_psql_db_info()
        psql_conn = cdb.psql_init_connect()

    select_sql = '''SELECT {col}
                    FROM {table}
                    WHERE entrez_id = {ent};
                    '''.format(col=column,table=diagnosis,ent=entrez_id)
    cur = psql_conn.cursor()
    cur.execute(select_sql)
    result = cur.fetchone()[0]
    if result is None:
        result = 'No data exists for this gene.'
    cur.close()
    return result

# returns uniprot ID and gene name given Entrez ID
def get_gene_info(entrez_id, collection):
    return collection.find_one({'_id': entrez_id})

# returns patient information (id, age, gender, education)
def get_patient_info(patient_id, collection):
    return collection.find_one({'_id': patient_id})
