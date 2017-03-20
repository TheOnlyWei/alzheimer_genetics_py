INSTALL DEPENDENCIES

PostgreSQL:
https://www.postgresql.org/download/

MongoDB
https://www.mongodb.com/download-center

pip
https://pip.pypa.io/en/stable/installing/

How to install python packages using pip
https://packaging.python.org/installing/

psycopg2
$sudo pip3 install psycopg2

pymongo
$sudo pip3 install pymongo


INFORMATION ABOUT THE PROGRAM

1.  FLAT FILES: To set up this part of the database, you need the files:
    - entrez_ids_uniprot.txt: the file containing columns entrez_id, uniprot_id,
      and Gene Name
    - patients.csv: the file containing patient_id, age, gender, and education.
    - ROSMAP_RNASeq_entrez.csv: the file containing patient_id, diagnosis, and
      16,380 entrez_id columns describing the patient's gene expression profile.

    They don't need to have those exact names, but make sure you enter the
      corresponding file paths when prompt for them, respectively.

    All ID entries must exist, otherwise there will be
      an error.

2.  SETUP: entry for database initialization is "main.py". This group of modules
      sets up the PostgreSQL and MongoDB databases for files from 1 (above).

      - Run $python3 main.py

3.  QUERIES: after the database is set up, you can query your data using methods
      defined in "query.py". The other files contain extraction, transformation
      and loading operations as well as code to initialize your database.

4.  TEST DATA: this repository comes with simplified versions of the files from
      part 1 in "data/" folder.
      - t0.csv, t1.csv, t2.csv: different simplified versions of
        ROSMAP_RNASeq_entrez.csv
      - eiu.csv: simplified version of entrez_ids_uniprot.txt
      - pat.csv: simplified version of patients.csv
