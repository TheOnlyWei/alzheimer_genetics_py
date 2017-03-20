import pandas as pd
import numpy as np
import os
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
                print('Error: patient ID already exists.')
    return True

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
                print('Error: patient ID already exists.')
    return True

def entrez_uniprot_file_insert(in_file,
                                delimiter,
                                header,
                                db_collection):

    if delimiter == 't':
        delimiter = '\t'
    elif delimiter == '\' \'':
        delimiter = ' '
    else:
        print('Error: unrecognized delimiter.')
        return False

    with open(in_file, 'r') as fi:
        if header:
            next(fi)
        for line in fi:
            cur_line = line
            cur_line = cur_line.replace('\n', '')
            data = re.split(r''+delimiter, cur_line)
            for index, info in enumerate(data):
                if info == '' or info is None:
                    data[index] = 'NA'
            exists = db_collection.find_one({'_id': data[0]})

            if exists is not None:
                exists = db_collection.find({'uniprot_id': {'$elemMatch': data[1]}})
                if not exists:
                    result = db_collection.update(
                        {'_id': data[0]},
                        {'$push': {'uniprot_id': data[1]}},
                        upsert=True
                    )
            else:
                result = db_collection.insert_one({
                    '_id': data[0],
                    'uniprot_id': [data[1]],
                    'gene_name': data[2],
                })
    return True

def patient_gene_expr_insert(data, delimiter, db_collection, psql_conn):
    update_running_stat_tables(data, delimiter, psql_conn)
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

#UPDATES
def running_mean_std(prev_mean, prev_std, n, new_data):
    if prev_mean == 'nan' or prev_std == 'nan' or new_data == 'nan':
        return {'mean':prev_mean, 'std_pop': std}

    mean = (new_data/(n+1.0)) + prev_mean*(n/(n+1.0))
    std = ((n/(n+1.0)) * ((prev_std**2.0) + ((prev_mean-new_data)**2.0)/(n+1.0)))**0.5
    result = {'mean': mean, 'std_pop': std}
    return result

def update_stat(select_sql, update_sql, table, psql_conn, new_data_arr):
    cur = psql_conn.cursor()

    cur.execute(select_sql.format(c='size',t=table))
    size = [expr[0] for expr in cur]

    if not size: # database empty, nothing to update
        return False

    cur.execute(select_sql.format(c='mean',t=table))
    mean = [expr[0] for expr in cur]

    cur.execute(select_sql.format(c='std_pop',t=table))
    std_pop = [expr[0] for expr in cur]

    # use the AD table to get list of entrez_ids
    cur.execute(select_sql.format(c='entrez_id',t='AD'))
    entrez_id = [expr[0] for expr in cur]

    id_size_mean_std = []

    for m,std,n,g,gid in zip(mean, std_pop ,size, new_data_arr, entrez_id):
        #print('mean: ', m, ' std: ',std, ' size: ', n, ' data: ', g, ' gid: ', gid)
        if g == 'nan':
            continue
        if m is not None and std is not None and n is not None and g is not None:
            mean_std = running_mean_std(float(m), float(std), int(n), float(g))
        elif (m is not None and std is None) or (m is None and std is not None):
            print('Error: incoherent input data, either mean or std is missing.')
            print('data not inserted.')
            return False
        if m is None and std is None:
            new_mean = g
            new_std = 0.0
        else:
            new_mean = mean_std['mean']
            new_std = mean_std['std_pop']
        n += 1
        #print('new_mean: ', new_mean, ' new_std: ', new_std)
        new_tuple = (gid, n, new_mean, new_std)
        id_size_mean_std.append(new_tuple)
        print()

    print(id_size_mean_std)
    cur.execute(update_sql.format(t=table), (id_size_mean_std,))
    #psql_conn.commit()
    cur.close()
    return True

def update_running_stat_tables(data, delimiter, psql_conn):
    data_arr = data.strip().split(delimiter)
    gene_expr = [float(n) if n!='' else 'nan' for n in data_arr[2:]]
    NCI = [1]
    MCI = [2,3]
    AD = [4,5]
    other = [6]

    select_sql = '''
                SELECT {c} FROM {t} ORDER BY entrez_id;
                '''

    update_sql = '''
                UPDATE {t}
                SET size = n, mean = m, std_pop = std
                FROM unnest(%s) s(gd integer, n integer, m numeric, std numeric)
                WHERE {t}.entrez_id = s.gd;
                '''

    if data_arr[1].isdigit():
        if int(data_arr[1]) in NCI:
            success = update_stat(select_sql, update_sql, 'NCI', psql_conn, gene_expr)
        elif int(data_arr[1]) in MCI:
            success = update_stat(select_sql, update_sql, 'MCI', psql_conn, gene_expr)
        elif int(data_arr[1]) in AD:
            success = update_stat(select_sql, update_sql, 'AD', psql_conn, gene_expr)
        elif int(data_arr[1]) in other:
            success = update_stat(select_sql, update_sql, 'other', psql_conn, gene_expr)
    elif data_arr[1] == '' or data_arr[1] == 'NA':
        success = update_stat(select_sql, update_sql, 'NA', psql_conn, gene_expr)
    else :
        print('ERROR: unrecognized diagnosis {d}.'.format(d=data_arr[1]))
        return False

    return success

def running_stat_file_insert(in_file, delimiter, header, psql_conn):
    if delimiter == '\t' or delimiter == 't':
        df = pd.read_table(in_file)
    elif delimiter == ',':
        df = pd.read_csv(in_file)
    elif delimiter == ' ' or delimiter == '\' \'':
        df = pd.read_csv(in_file, delim_whitespace=True)
    else:
        print('Error: unrecognized delimiter.')
        return False

    diagnosis = ['nci','mci','ad','other','na']
    NCI = [1]
    MCI = [2,3]
    AD = [4,5]
    other = [6]

    #TODO: make this sql update instead of DO NOTHING
    insert_sql = '''
                INSERT INTO {t} (entrez_id,size,mean,std_pop)
                VALUES (%s,%s,NULLIF(%s, 'nan'), NULLIF(%s, 'nan'))
                ON CONFLICT (entrez_id)
                DO UPDATE
                SET size = EXCLUDED.size, mean = EXCLUDED.size, std_pop = EXCLUDED.size;
                '''
    cur = psql_conn.cursor()

    for table in diagnosis:
        cur.execute('SELECT exists(SELECT * from information_schema.tables WHERE table_name=%s)', (table,))
        if not cur.fetchone()[0]:
            print('A table named {t} does not exist.'.format(t=table))
            print('File import aborted.')
            return False

    # Loop through all gene columns
    for col in df.ix[:,2:]:
        size = df.loc[df['DIAGNOSIS'].isin(NCI), col].dropna().size
        mean = df.loc[df['DIAGNOSIS'].isin(NCI), col].mean()
        std = df.loc[df['DIAGNOSIS'].isin(NCI), col].std(ddof=0)
        cur.execute(insert_sql.format(t='nci'),(int(col),size,mean,std))

        size = df.loc[df['DIAGNOSIS'].isin(MCI), col].dropna().size
        mean = df.loc[df['DIAGNOSIS'].isin(MCI), col].mean()
        std = df.loc[df['DIAGNOSIS'].isin(MCI), col].std(ddof=0)
        cur.execute(insert_sql.format(t='mci'),(int(col),size,mean,std))

        size = df.loc[df['DIAGNOSIS'].isin(AD), col].dropna().size
        mean = df.loc[df['DIAGNOSIS'].isin(AD), col].mean()
        std = df.loc[df['DIAGNOSIS'].isin(AD), col].std(ddof=0)
        cur.execute(insert_sql.format(t='ad'),(int(col),size,mean,std))

        size = df.loc[df['DIAGNOSIS'].isin(other), col].dropna().size
        mean = df.loc[df['DIAGNOSIS'].isin(other), col].mean()
        std = df.loc[df['DIAGNOSIS'].isin(other), col].std(ddof=0)
        cur.execute(insert_sql.format(t='other'),(int(col),size,mean,std))

        size = df.loc[df['DIAGNOSIS'].isnull(), col].dropna().size
        mean = df.loc[df['DIAGNOSIS'].isnull(), col].mean()
        std = df.loc[df['DIAGNOSIS'].isnull(), col].std(ddof=0)
        cur.execute(insert_sql.format(t='NA'),(int(col),size,mean,std))

    #psql_conn.commit()
    cur.close()
    return True
