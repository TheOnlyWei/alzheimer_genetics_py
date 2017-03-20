import create_db as cdb
import transform_load as tl
from pymongo import MongoClient

def main():
    #DATABASE INITIALIZATION

    print('''Here we set up PostgreSQL database for the
            1. Entrez ID to Uniprot ID and gene information table.\n
            ''')

    info = cdb.get_psql_db_info()
    psql_conn = cdb.psql_init_connect(info) # also sets up database
    cdb.create_entrez_uniprot_table('entrez_uniprot', psql_conn)

    print()

    file_path = input('Enter the file path for Entrez ID, Uniprot ID, and gene info.: ')
    delim = input('Enter the delimiter (comma: ",", tab: "t", space: " "): ')
    if tl.entrez_uniprot_file_insert(file_path, delim, 'entrez_uniprot', psql_conn):
        print('SUCCESS: Entrez ID, Uniprot ID, Gene Name file imported.')

    print()

    print('''Here we set up MongoDB collections for the
            2. Patient gene expression profile file.
            3. Patient age, gender, and education file.\n
            ''')

    mongo_client = MongoClient()

    gene_expr_collection = cdb.get_mongodb_collection('alzheimer_genetics', 'patient_gex', mongo_client)
    print('Creating MongoDB database "alzheimer_genetics"...\n')

    file_path = input('Enter the file path for patient gene expression profile: ')
    delim = input('Enter the delimiter: ')
    tl.patient_gene_expr_file_insert(file_path, delim, True, gene_expr_collection)

    patients_collection = cdb.get_mongodb_collection('alzheimer_genetics', 'patients', mongo_client)
    file_path = input('Enter the file path for patient age, gender, and education: ')
    delim = input('Enter the delimiter: ')
    tl.patient_info_file_insert(file_path, delim, True, patients_collection)

if __name__ == '__main__':
    main()
