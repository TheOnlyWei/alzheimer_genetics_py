import pandas as pd
import numpy as np
import itertools
import pymongo
import psycopg2
import math
import re

#BATCH INSERT FILE UPLOAD
def patient_gene_expr_file_insert(in_file,
                                    delimiter,
                                    header,
                                    db_collection):
    if delimiter == 't':
        delimiter = '\t'
    elif delimiter == '\' \'':
        delimiter = ' '
    elif delimiter != ',':
        print('Error: unrecognized delimiter.')
        return False

    with open(in_file, 'r') as fi:
        if header:
            next(fi)
        for line in fi:
            #data = line.strip().split(delimiter)
            cur_line = line
            cur_line = cur_line.replace('\n', '')
            data = re.split(r''+delimiter, cur_line)
            if data[1] == '':
                data[1] = 'NA'

            exists = db_collection.find_one({'_id': data[0]})
            if not exists:
                result = db_collection.insert_one({
                    '_id': data[0],
                    'diagnosis': data[1],
                    'gene_expression': data[2:]
                })
            else:
                print('Error: patient ID {id} already exists.'.format(id=data[0]))

def patient_info_file_insert(in_file,
                                delimiter,
                                header,
                                db_collection):
    if delimiter == 't':
        delimiter = '\t'
    elif delimiter == '\' \'':
        delimiter = ' '
    elif delimiter != ',':
        print('Error: unrecognized delimiter.')
        return False

    with open(in_file, 'r') as fi:
        if header:
            next(fi)
        for line in fi:
            #data = line.strip().split(delimiter)
            cur_line = line
            cur_line = cur_line.replace('\n', '')
            data = re.split(r''+delimiter, cur_line)
            for index, info in enumerate(data):
                if info == '':
                    data[index] = 'NA'

            exists = db_collection.find_one({'_id': data[0]})
            if not exists:
                result = db_collection.insert_one({
                    '_id': data[0],
                    'age': data[1],
                    'gender': data[2],
                    'education': data[3]
                })
            else:
                print('Error: patient ID {id} already exists.'.format(id=data[0]))

def entrez_uniprot_file_insert(in_file,
                                delimiter,
                                table,
                                psql_conn,
                                header = True):
    if delimiter == 't':
        delimiter = '\t'
    elif delimiter == '\' \'':
        delimiter = ' '
    elif delimiter != ',' and delimiter != '\t' and delimiter != ' ':
        print('Error: unrecognized delimiter.')
        return False

    insert_sql = '''
                INSERT INTO {t} (entrez_id, uniprot_id, gene_name)
                VALUES (%s, %s, %s)
                '''.format(t=table)
    select_gene_sql = '''
                        SELECT gene_name FROM entrez_uniprot WHERE entrez_id={id};
                        '''
    update_sql_with_gene = '''
                UPDATE entrez_uniprot SET uniprot_id = array_append(uniprot_id, {d}), gene_name = {n} WHERE entrez_id = {id};
                '''
    update_sql_no_gene = '''
                UPDATE entrez_uniprot SET uniprot_id = array_append(uniprot_id, {d}) WHERE entrez_id = {id};
                '''
    cur = psql_conn.cursor()

    with open(in_file, 'r') as fi:
        if header:
            next(fi)
        for line in fi:
            cur_line = line
            cur_line = cur_line.replace('\n', '')
            data = re.split(r''+delimiter, cur_line)

            select_entrez_sql = 'SELECT entrez_id FROM entrez_uniprot WHERE entrez_id={id};'.format(id=int(data[0]))
            cur.execute(select_entrez_sql)
            entrez_id = cur.fetchone()
            if cur.rowcount > 0: # a corresponding entrez_id value exists
                select_entrez_uniprot_sql = 'SELECT uniprot_id FROM entrez_uniprot WHERE \'{d}\'=ANY(uniprot_id) and entrez_id={id};'.format(d=data[1],id=int(data[0]))
                cur.execute(select_entrez_uniprot_sql)
                if cur.rowcount > 0:
                    duplicate_data = cur.fetchone()
                    print('ERROR: duplicate data {d} for entrez ID {id} is not inserted'.format(d=duplicate_data[0], id=entrez_id[0]))
                else:
                    for index, info in enumerate(data):
                        if info == '' or info is None:
                            data[index] = 'NA'
                    uniprot_id = '\'' + data[1] + '\''
                    cur.execute(select_gene_sql.format(id=int(data[0])))
                    gene_name_result = cur.fetchone()
                    if gene_name_result[0] == '':
                        new_gene_name = '\'' + data[2] + '\''
                        cur.execute(update_sql_with_gene.format(d=uniprot_id,n=new_gene_name,id=int(data[0])))
                    else:
                        cur.execute(update_sql_no_gene.format(d=uniprot_id,id=int(data[0])))
            else: # not contained at all, so insert
                uniprot_id = '{' + data[1] + '}'
                cur.execute(insert_sql, (int(data[0]),uniprot_id,data[2]))

    psql_conn.commit()
    cur.close()

def patient_gene_expr_insert(data, delimiter, db_collection, psql_conn):
    #update_running_stat_tables(data, delimiter, psql_conn)
    clean_data = data
    clean_data = clean_data.replace('\n', '')
    clean_data = re.split(r''+delimiter, clean_data)

    exists = db_collection.find_one({'_id': clean_data[0]})
    if not exists:
        result = db_collection.insert_one({
            '_id': clean_data[0],
            'diagnosis': clean_data[1],
            'gene_expression': clean_data[2:]
        })
        return True
    else:
        print('Error: patient ID already exists.')
        return False

def patient_info_insert(data, delimiter, db_collection):
    clean_data = data
    clean_data = clean_data.replace('\n', '')
    clean_data = re.split(r''+delimiter, clean_data)
    for index, info in enumerate(clean_data):
        if info == '':
            clean_data[index] = 'NA'

    exists = db_collection.find_one({'_id': clean_data[0]})
    if not exists:
        result = db_collection.insert_one({
            '_id': clean_data[0],
            'age': clean_data[1],
            'gender': clean_data[2],
            'education': clean_data[3]
        })
        return True
    else:
        print('Error: patient ID already exists.')
        return False
