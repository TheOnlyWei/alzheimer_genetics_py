import pandas as pd
import numpy as np
import itertools
import psycopg2
import math
import re
import create_db as cdb

def check_delimiter(delimiter):
    if delimiter == 't':
        return '\t'
    elif delimiter == '\' \'':
        return ' '
    elif delimiter != ',' and delimiter != '\t' and delimiter != ' ':
        return None
        print('Error: unrecognized delimiter.')
    return delimiter

def pandas_parse(in_file, delimiter, header = True):
    if header:
        if delimiter == ',':
            return pd.read_csv(in_file, dtype={'DIAGNOSIS':object})
        elif delimiter == 't' or delimiter == '\t':
            return pd.read_table(in_file, dtype={'DIAGNOSIS':object})
        elif delimiter == '\' \'' or  delimiter == ' ':
            return pd.read_csv(in_file, delim_whitespace=True, dtype={'DIAGNOSIS':object})
        else:
            print('Error: unrecognized delimiter.')
    else:
        if delimiter == ',':
            return pd.read_csv(in_file, dtype={'DIAGNOSIS':object}, header=None)
        elif delimiter == 't' or delimiter == '\t':
            return pd.read_table(in_file, dtype={'DIAGNOSIS':object}, header=None)
        elif delimiter == '\' \'' or  delimiter == ' ':
            return pd.read_csv(in_file, delim_whitespace=True, dtype={'DIAGNOSIS':object}, header=None)
        else:
            print('Error: unrecognized delimiter.')
    return None
#BATCH INSERT FILE UPLOAD
#patient gene expression profile and patient info should be parsed together
def patient_gene_expr_file_insert(in_file, delimiter, psql_conn, header = True):
    print('Importing file {f}.'.format(f=in_file))
    print('If there is a conflict of patient ID, the program will replace the old one.')
    df = pandas_parse(in_file, delimiter)
    if df is None:
        print('df: ', df)
        return False

    df = df.fillna('NULL')
    NCI = [1]
    MCI = [2,3]
    AD = [4,5]
    other = [6]

    insert_gene = '''
                    INSERT INTO {t} (patient_id, gene_expression)
                    VALUES (%s,%s)
                    ON CONFLICT (patient_id)
                    DO UPDATE
                    SET gene_expression = EXCLUDED.gene_expression;
                '''
    insert_patient = '''INSERT INTO patients (patient_id, diagnosis)
                        VALUES (%s,%s)
                        ON CONFLICT (patient_id)
                        DO UPDATE
                        SET diagnosis = EXCLUDED.diagnosis;
                    '''

    cur = psql_conn.cursor()
    cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)',('patients',))
    if not cur.fetchone()[0]:
        cdb.create_patients_table('patients', psql_conn)
    else:
        cur.close()
        return False

    for index, row in df.iterrows():
        arr = row[2:].tolist()
        postgres_arr = '{' + ','.join(map(str, arr)) + '}'
        diagnosis = None if row['DIAGNOSIS'] == 'NULL' else row['DIAGNOSIS']
        cur.execute(insert_patient, (row['PATIENT_ID'], diagnosis,))
        if row['DIAGNOSIS'].isdigit():
            if int(row['DIAGNOSIS']) in NCI:
                cur.execute(insert_gene.format(t='nci'), (row['PATIENT_ID'], postgres_arr,))

            elif int(row['DIAGNOSIS']) in MCI:
                cur.execute(insert_gene.format(t='mci'), (row['PATIENT_ID'], postgres_arr,))

            elif int(row['DIAGNOSIS']) in AD:
                cur.execute(insert_gene.format(t='ad'), (row['PATIENT_ID'], postgres_arr,))

            elif int(row['DIAGNOSIS']) in other:
                cur.execute(insert_gene.format(t='other'), (row['PATIENT_ID'], postgres_arr,))

        elif row['DIAGNOSIS'] == 'NULL': # insert into table NA
            cur.execute(insert_gene.format(t='na'), (row['PATIENT_ID'], postgres_arr,))

        else:
            print('ERROR: unknown diagnosis {d}.'.format(d=diagnosis))

    psql_conn.commit()
    cur.close()
    return True

#patient gene expression profile and patient info should be parsed together
def patient_info_file_insert(in_file,
                                delimiter,
                                psql_conn,
                                header = True,):
    print('Importing file {f}.'.format(f=in_file))
    print('If there is a conflict of patient ID, the program will replace the old one.')
    df = pandas_parse(in_file, delimiter)
    if df is None:
        return False
    df = df.fillna('NULL')

    insert_sql = '''INSERT INTO patients (patient_id, age, gender, education)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (patient_id)
                    DO UPDATE
                    SET age = EXCLUDED.age, gender = EXCLUDED.gender, education = EXCLUDED.education;
                    '''
    cur = psql_conn.cursor()

    for index, row in df.iterrows():
        age = None if row['age'] == 'NULL' else row['age']
        gender = None if row['gender'] == 'NULL' else row['gender']
        education = None if row['education'] == 'NULL' else row['education']
        cur.execute(insert_sql, (row['patient_id'], age, gender, education))

    psql_conn.commit()
    cur.close()
    return True


def entrez_uniprot_file_insert(in_file,
                                delimiter,
                                psql_conn,
                                header = True):
    print('Importing file {f}.'.format(f=in_file))
    print('If there is a conflict of entrez ID, the program will update missing values.')
    delimiter = check_delimiter(delimiter)
    if delimiter is None:
        return False

    insert_sql = '''
                INSERT INTO entrez_uniprot (entrez_id, uniprot_id, gene_name)
                VALUES (%s, %s, %s)
                '''
    select_gene_sql = '''
                        SELECT gene_name FROM entrez_uniprot WHERE entrez_id=%s;
                        '''
    update_sql_with_gene = '''
                UPDATE entrez_uniprot SET uniprot_id = array_append(uniprot_id, {d}), gene_name = {n} WHERE entrez_id = %s;
                '''
    update_sql_no_gene = '''
                UPDATE entrez_uniprot SET uniprot_id = array_append(uniprot_id, {d}) WHERE entrez_id = %s;
                '''
    cur = psql_conn.cursor()

    with open(in_file, 'r') as fi:
        if header:
            next(fi)
        for line in fi:
            cur_line = line
            cur_line = cur_line.replace('\n', '')
            data = re.split(r''+delimiter, cur_line)
            # SQL for finding if entrez ID is already in the table
            select_entrez_sql = 'SELECT entrez_id FROM entrez_uniprot WHERE entrez_id=%s;'
            cur.execute(select_entrez_sql, (data[0],))
            entrez_id = cur.fetchone()
            if cur.rowcount > 0: # a corresponding entrez_id value exists
                select_entrez_uniprot_sql = 'SELECT uniprot_id FROM entrez_uniprot WHERE %s=ANY(uniprot_id) and entrez_id=%s;'
                cur.execute(select_entrez_uniprot_sql, (data[1], data[0],))
                if cur.rowcount <= 0: # the uniprot ID doesn't exist, so insert
                    for index, info in enumerate(data):
                        if info == '' or info is None:
                            data[index] = 'NULL'
                    uniprot_id = '\'' + data[1] + '\''
                    cur.execute(select_gene_sql, (data[0],)) # insert uniprot_id
                    gene_name_result = cur.fetchone()
                    if gene_name_result[0] == '':
                        new_gene_name = '\'' + data[2] + '\''
                        cur.execute(update_sql_with_gene.format(d=uniprot_id,n=new_gene_name),(data[0],))
                    else:
                        cur.execute(update_sql_no_gene.format(d=uniprot_id), (data[0],))
            else: # not contained at all, so insert
                uniprot_id = '{' + data[1] + '}'
                cur.execute(insert_sql, (int(data[0]),uniprot_id,data[2]))

    psql_conn.commit()
    cur.close()
    return True
