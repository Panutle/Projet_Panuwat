import psycopg2
from pymongo import MongoClient, InsertOne
from datetime import datetime

#Connect Postgresql
HOST_PG ='localhost'
PORT_PG = '5432'
DATABASE_PG = 'test_pg'
USER_PG = 'postgres'
PASSWORD_PG = 'zxcvbnm'

#Connect MongoDB
USER_MG = 'mongo_demo'
PASSWORD_MG = 'zxcvbnm'
IP_MG = 'localhost'
PORT_MG = '27017'
replcasetname = ''

mongo_db = 'db_demo'
mongo_collection = 'col_demo'

def que_data():
    connection = psycopg2.connect(
                host = HOST_PG,
                port = PORT_PG,
                database = DATABASE_PG,
                user = USER_PG,
                password = PASSWORD_PG
    )
    cursor = connection.cursor()

    query = f"""
        SELECT 
        FROM ;
        """

    cursor.execute(query)
    pg_data = cursor.fetchall()
    print(datetime.now(), " Data len :", len(pg_data))
    cursor.close()
    connection.close()

    return pg_data

def conn_mongo():
    if replcasetname.strip() == '':
        mongo_host = f"mongodb://{USER_MG}:{PASSWORD_MG}@{IP_MG}:{PORT_MG}/"
        
    else:
        mongo_host = f"mongodb://{USER_MG}:{PASSWORD_MG}@{IP_MG}:{PORT_MG}/{mongo_db}?replicaSet={replcasetname}"

    client = MongoClient(f"{mongo_host}")
    db = client[f'{mongo_db}']
    collection = db[f'{mongo_collection}']

    return db, collection, mongo_collection

def up_to_mongo(data, collection, db):
    print('#### DROP COLLECTION AT MONGO ####')
    collection.drop()

    print(f'#### CREATE COLLECTION AT MONGO {mongo_collection} ####')
    db.create_collection(f'{mongo_collection}')


    print('#### START BULK WRITE TO MONGO ####')
    bulk_updates = []

    i = 0

    for row in data:
        user_id = str(row[0])
        ranking = row[1]
        sku_code = row[2]
        create_info_timestamp = row[3].strftime("%Y-%m-%d %H:%M:%S")

        document = {
            'user_id': user_id,
            'ranking': ranking,
            'sku_code': sku_code,
            'create_info_timestamp':create_info_timestamp
        }

        bulk_updates.append(InsertOne(document))

        i = i+1

        #ส่งทุกๆ 1000 แถว
        if i % 1000 == 0:
            collection.bulk_write(bulk_updates)
            bulk_updates = []
        elif i == len(data):
            collection.bulk_write(bulk_updates)
            bulk_updates = []

def Check(data, collection):
    total_documents_after_insert = collection.count_documents({})

    if total_documents_after_insert == len(data):
        print(f"Number of documents in {mongo_collection} collection equal fecth data = {total_documents_after_insert}.")
    else:
        print(f"ERROR: Number of documents in {mongo_collection} collection does not equal fecth data {total_documents_after_insert} !=  {len(data)}.")


if __name__ == "__main__":
    starttime = datetime.now()
    print(f'Start Connect Postgresql: {starttime}')

    data = que_data()

    con_mg = datetime.now()
    print(f'Start Connect MongoDB: {con_mg}')
    db, collection, mongo_collection = conn_mongo()

    up_to_mongo(data, collection, db)

    endtime = datetime.now()
    duration = endtime - starttime
    print(f'End Time: {endtime}')
    print(f'Duration: {duration}')

    Check(data, collection)
