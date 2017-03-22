import create_db as cdb
import transform_load as tl

def main():
    #DATABASE INITIALIZATION

    print('''Here we set up PostgreSQL database for the
            1. Entrez ID to Uniprot ID and gene information table.
            2. Patient gene expression profile file.
            3. Patient age, gender, and education file.\n
            ''')

    info = cdb.get_psql_db_info()
    psql_conn = cdb.psql_init_connect(info) # also sets up database
    
    print()

    file_path = input('Enter the file path for Entrez ID, Uniprot ID, and gene info.: ')
    delim = input('Enter the delimiter (comma: ",", tab: "t", space: " "): ')
    cdb.create_entrez_uniprot_table(psql_conn)
    tl.entrez_uniprot_file_insert(file_path, delim, psql_conn)

    print()

    file_path = input('Enter the file path for patient age, gender, and education: ')
    delim = input('Enter the delimiter (comma: ",", tab: "t", space: " "): ')
    cdb.create_patients_table(psql_conn)
    tl.patient_info_file_insert(file_path, delim, psql_conn)

    print()

    file_path = input('Enter the file path for patient gene expression profile: ')
    delim = input('Enter the delimiter (comma: ",", tab: "t", space: " "): ')
    cdb.create_diagnosis_tables(psql_conn)
    if tl.patient_gene_expr_file_insert(file_path, delim, psql_conn):
        cdb.create_entrez_id_to_index(file_path, delim, psql_conn)



if __name__ == '__main__':
    main()
