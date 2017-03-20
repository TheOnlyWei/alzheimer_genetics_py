import create_db as cdb
import transform_load as tl
from pymongo import MongoClient

def main():
    #DATABASE INITIALIZATION

    print('''Here we set up PostgreSQL database for the
            1. Running statistic tables NA, NCI, MCI, AD, and other.
            2. Entrez ID to Uniprot ID and gene information table.
            ''')

    info = cdb.get_psql_db_info()
    psql_conn = cdb.psql_init_connect(info) # also sets up database
    cdb.create_running_stat_table(psql_conn)

    print()

    print('''Here we set up MongoDB collections for the
            3. Patient gene expression profile file.
            4. Patient age, gender, and education file.
            ''')

    mongo_client = MongoClient()
    mongo_db = cdb.mongo_db_init(mongo_client)

    entrez_uniprot_collection = cdb.entrez_uniprot_init(mongo_db)
    file_path = input('Enter the file path for Entrez ID, Uniprot ID, and gene info.: ')
    delim = input('Enter the delimiter (comma: ",", tab: "t", space: " "): ')
    if tl.entrez_uniprot_file_insert(file_path, delim, True, entrez_uniprot_collection):
        print('SUCCESS: Entrez ID, Uniprot ID, Gene Name file imported.')

    print()

    gene_expr_collection = cdb.gene_expr_init(mongo_db)
    file_path = input('Enter the file path for patient gene expression profile: ')
    delim = input('Enter the delimiter: ')
    if tl.running_stat_file_insert(file_path, delim, True, psql_conn):
        tl.patient_gene_expr_file_insert(file_path, delim, True, gene_expr_collection)
        print('SUCCESS: patient gene expression profile imported')
    else:
        print('ERROR: due to merge conflict of existing tables the patient')
        print('gene expression profile has not been imported.')

    print()

    patients_collection = cdb.patients_init(mongo_db)
    file_path = input('Enter the file path for patient age, gender, and education: ')
    delim = input('Enter the delimiter: ')
    if tl.patient_info_file_insert(file_path, delim, True, patients_collection):
        print('SUCCESS: patient information data imported.')


    psql_conn.commit()

if __name__ == '__main__':
    main()
