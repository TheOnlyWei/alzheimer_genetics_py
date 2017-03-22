import re
import transform_load as tl

# data is a string delimited by delimiter
def patient_gene_expr_insert(data, delimiter, psql_conn):
    delimiter = tl.check_delimiter(delimiter)
    clean_data = data
    clean_data = clean_data.replace('\n', '')
    clean_data = re.split(r''+delimiter, clean_data)
    clean_data = [x if x != '' and x.lower() != 'NA' else 'NULL' for x in clean_data]

    NCI = [1]
    MCI = [2,3]
    AD = [4,5]
    other = [6]

    select_sql = 'SELECT EXISTS(SELECT 1 FROM {t} WHERE patient_id = %s)'
    insert_sql = '''INSERT INTO {t} (patient_id, gene_expression)
                    VALUES (%s, %s)
                    ON CONFLICT (patient_id)
                    DO UPDATE
                    SET gene_expression = EXCLUDED.gene_expression;
                '''
    cur = psql_conn.cursor()
    if clean_data[1].isdigit():
        if int(clean_data[1]) in NCI:
            cur.execute(select_sql.format(t='nci'), (clean_data[0],))

        elif int(clean_data[1]) in MCI:
            cur.execute(select_sql.format(t='mci'), (clean_data[0],))

        elif int(clean_data[1]) in AD:
            cur.execute(select_sql.format(t='ad'), (clean_data[0],))

        elif int(clean_data[1]) in other:
            cur.execute(select_sql.format(t='other'), (clean_data[0],))
    elif clean_data[1] is None: # insert into table NA
        cur.execute(insert_sql.format(t='na'), (clean_data[0], postgres_arr,))
    else:
        cur.close()
        print('ERROR: unknown diagnosis {d}.'.format(d=diagnosis))
        return False

    if cur.fetchone()[0]:
        print('ERROR: patient id {id} exists.'.format(id=clean_data[0]))
        replace = input('Do you want to replace the data (y/n)? ')
        replace = replace.lower()
        if replace == 'n':
            cur.close()
            return False

    arr = clean_data[2:]

    postgres_arr = '{' + ','.join(map(str, arr)) + '}'
    if clean_data[1].isdigit():
        if int(clean_data[1]) in NCI:
            cur.execute(insert_sql.format(t='nci'), (clean_data[0], postgres_arr,))

        elif int(clean_data[1]) in MCI:
            cur.execute(insert_sql.format(t='mci'), (clean_data[0], postgres_arr,))

        elif int(clean_data[1]) in AD:
            cur.execute(insert_sql.format(t='ad'), (clean_data[0], postgres_arr,))

        elif int(clean_data[1]) in other:
            cur.execute(insert_sql.format(t='other'), (clean_data[0], postgres_arr,))

    elif clean_data[1] == '' or clean_data[1].lower() == 'na': # insert into table NA
        cur.execute(insert_sql.format(t='na'), (clean_data[0], postgres_arr,))

    psql_conn.commit()
    cur.close()
    return True

# data is a string delimited by delimiter
def patient_info_insert(data, delimiter, psql_conn):
    delimiter = tl.check_delimiter(delimiter)
    clean_data = data
    clean_data = clean_data.replace('\n', '')
    clean_data = re.split(r''+delimiter, clean_data)
    clean_data = [x if x != '' and x.lower() != 'NA' else 'NULL' for x in clean_data]

    NCI = [1]
    MCI = [2,3]
    AD = [4,5]
    other = [6]

    select_sql = 'SELECT EXISTS(SELECT 1 FROM patients WHERE patient_id = %s)'

    insert_sql = '''INSERT INTO patients (patient_id, age, gender, education)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (patient_id)
                    DO UPDATE
                    SET age = EXCLUDED.age, gender = EXCLUDED.gender, education = EXCLUDED.education;
                '''
    cur = psql_conn.cursor()
    cur.execute(select_sql, (clean_data[0],))

    if cur.fetchone()[0]:
        print('ERROR: patient ID {id} exists.'.format(id=clean_data[0]))
        replace = input('Do you want to replace the data (y/n)?')
        replace = replace.lower()
        if replace == 'n':
            return False

    cur.execute(insert_sql, (clean_data[0],clean_data[2],clean_data[3], clean_data[4],))
    psql_conn.commit()
    cur.close()
    return True

# TODO: entrez_uniprot table insert
def entrez_uniprot_insert(data, delimiter, psql_conn):
    delimiter = tl.check_delimiter(delimiter)
    clean_data = data
    clean_data = clean_data.replace('\n', '')
    clean_data = re.split(r''+delimiter, clean_data)
    clean_data = [x if x != '' and x.lower() != 'NA' else 'NULL' for x in clean_data]
    clean_data[1] = clean_data[1].replace(' ', '')
    
    select_entrez_sql = 'SELECT EXISTS(SELECT 1 FROM entrez_uniprot WHERE entrez_id = %s)'
    select_uniprot_sql = 'SELECT uniprot_id FROM entrez_uniprot WHERE %s=ANY(uniprot_id) and entrez_id=%s;'

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
    cur.execute(select_entrez_sql, (clean_data[0],))

    if cur.fetchone()[0]: # if entrez ID is already in table
        cur.execute(select_uniprot_sql, (clean_data[1], clean_data[0],))
        if cur.rowcount <= 0: # if uniprot_id not in array
            uniprot_id = '\'' + clean_data[1] + '\''
            cur.execute(select_gene_sql, (clean_data[0],)) # insert uniprot_id
            gene_name_result = cur.fetchone()
            print(clean_data[1])
            if gene_name_result[0] == '':
                new_gene_name = '\'' + clean_data[2] + '\''
                cur.execute(update_sql_with_gene.format(d=uniprot_id,n=new_gene_name),(clean_data[0],))
            else:
                cur.execute(update_sql_no_gene.format(d=uniprot_id), (clean_data[0],))
    else: # not contained at all, so insert
        uniprot_id = '{' + clean_data[1] + '}'
        cur.execute(insert_sql, (int(clean_data[0]),uniprot_id,clean_data[2]))

    psql_conn.commit()
    cur.close()
    return True
