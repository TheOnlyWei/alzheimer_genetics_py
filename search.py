import create_db as cdb

# psql_conn is the PostgreSQL database connection
def get_gene_stat(entrez_id, diagnosis, stat, psql_conn):
    """ Returns aggregate result for mean or population standard deviation

    diagnosis -- integers in [1,6] or string in ['nci', 'mci', 'ad', 'other', 'na']
    stat -- "mean" or "std_pop"
    """

    NCI = [1]
    MCI = [2,3]
    AD = [4,5]
    other = [6]
    diagnosis_arr = ['nci', 'mci', 'ad', 'other', 'na']
    if type(diagnosis) is int:
        if diagnosis in NCI:
            diagnosis = 'nci'
        elif diagnosis in MCI:
            diagnosis = 'mci'
        elif diagnosis in AD:
            diagnosis = 'ad'
        elif diagnosis in other:
            diagnosis = 'other'
    elif type(diagnosis) is str and diagnosis.lower() in diagnosis_arr:
        diagnosis = diagnosis.lower()
    else:
        print('ERROR: unknown diagnosis {d}.'.format(d=diagnosis))
        return False

    if type(stat) is not str:
        print('ERROR: unknown aggregate function {a}.'.format(a=stat))
        return False
    if type(entrez_id) is not int and type(entrez_id) is not str:
        print('ERROR: entrez ID must be an integer or string.')
        return False
    elif type(entrez_id) is int:
        entrez_id = str(entrez_id)

    if stat == 'mean':
        method = 'avg'
    elif stat == 'std_pop':
        method = 'stddev_pop'
    else:
        print('ERROR: unknown stat, currently we support "mean" or "std_pop"')
        return False

    select_sql = '''SELECT {m}(gene_expression[%s])
                    FROM {t}
                    '''.format(m=method,t=diagnosis)
    select_gene_index = '''SELECT index
                            FROM entrez_id_to_index
                            WHERE entrez_id = %s;
                        '''
    stat = stat.lower()
    if not psql_conn:
        info = cdb.get_psql_db_info()
        psql_conn = cdb.psql_init_connect()

    cur = psql_conn.cursor()
    cur.execute(select_gene_index, (entrez_id))
    index = cur.fetchone()

    if index is None:
        cur.close()
        print('ERROR: entrez ID doesn\'t exist.')
        return None

    index = index[0]
    cur.execute(select_sql, (str(index)))
    result = cur.fetchone()[0]

    if result is None:
        print('ERROR: no data exists for this gene.')
    cur.close()
    return result

# returns uniprot ID and gene name given Entrez ID
def get_gene_info(entrez_id, psql_conn):
    select_sql = '''SELECT uniprot_id, gene_name
                            FROM entrez_uniprot
                            WHERE entrez_id = %s;
                        '''
    if type(entrez_id) is not int and type(entrez_id) is not str:
        print('ERROR: entrez ID must be an integer or string.')
        return False
    elif type(entrez_id) is int:
        entrez_id = str(entrez_id)

    cur = psql_conn.cursor()
    cur.execute(select_sql, (entrez_id,))
    result = [x for x in cur.fetchone()]
    return result

# returns patient information (id, age, gender, education)
def get_patient_info(patient_id, psql_conn):
    select_sql = '''SELECT diagnosis, age, gender, education
                            FROM patients
                            WHERE patient_id = %s;
                        '''
    if type(patient_id) is not str:
        print('ERROR: patient ID must be a string.')
        return False

    cur = psql_conn.cursor()
    cur.execute(select_sql, (patient_id,))
    result = [x for x in cur.fetchone()]
    return result
