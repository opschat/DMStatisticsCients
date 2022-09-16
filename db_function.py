import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('var_cients.env')
load_dotenv(dotenv_path=dotenv_path)


database_ADV = os.environ.get('PG_DATABASE_ADVANCED')
host = os.environ.get('HOST_BD')
user_ADV = os.environ.get('PG_USER_ADVANCED')
password = os.environ.get('PG_PASSWORD')
port_ADV = os.environ.get('PG_PORT_ADVANCED')
database_DMcients = os.environ.get('PG_DATABASE_DMCIENTS')
user_DMcients = os.environ.get('PG_USER_DMCIENTS')
port_DMcients = os.environ.get('PG_PORT_DMCIENTS')


def conect_DBstatisticsByCients():
    con = psycopg2.connect(host=host,
                           port=port_ADV,
                           database=database_ADV,
                           user=user_ADV,
                           password=password)
    return con


def conect_DBDMstatisticsByCients():
    con = psycopg2.connect(host=host,
                           port=port_DMcients,
                           database=database_DMcients,
                           user=user_DMcients,
                           password=password)
    return con


def query_DBstatisticsByCients(sql):
    con = conect_DBstatisticsByCients()
    cur = con.cursor()
    cur.execute(sql)
    recset = cur.fetchall()
    registros = []
    for rec in recset:
        registros.append(rec)
    con.close()
    return registros


def query_DBDMCients(sql):
    con = conect_DBDMstatisticsByCients()
    cur = con.cursor()
    cur.execute(sql)
    recset = cur.fetchall()
    registros = []
    for rec in recset:
        registros.append(rec)
    con.close()
    return registros


def insert_DBDMcients(sql):
    con = conect_DBDMstatisticsByCients()
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()


def insert_DMcients(dataFrame):
    rows = int(dataFrame.shape[0])
    for i in range(0, rows):
        sql = """INSERT into clientsTags(id,id_client,client_name,company_token,quantity_services_opened,
quantity_message_sended,quantity_message_received,quantity_looked_by_tag,computed_date,createdAt,tags)
        values('%s','%s','%s','%s','%s','%s','%s','%s',to_timestamp('%s', 'DD/MM/YYYY'),to_timestamp('%s', 'DD/MM/YYYY'),'%s'); """ % (dataFrame['id'][i], dataFrame['id_client'][i], dataFrame['client_name'][i], dataFrame['company_token'][i], dataFrame['quantity_services_opened'][i], dataFrame['quantity_message_sended'][i], dataFrame['quantity_message_received'][i], dataFrame['quantity_looked_by_tag'][i], dataFrame['computed_date'][i], dataFrame['createdAt'][i], dataFrame['tags'][i])
        insert_DBDMcients(sql)


def searchLastId_DM():
    sql_searchIdClientstags = """ SELECT clientstags.id FROM clientstags ORDER BY id DESC limit 1; """
    lastID_db = query_DBDMCients(sql_searchIdClientstags)
    df_bd = pd.DataFrame(lastID_db, columns=['id'])
    id = df_bd['id'].to_string(index=False)
    id = int(id)
    return id


def catch_lastRecordStatisticsByCients(lastIdDM):

    sql_select_all = f"""select * from "statisticsByCients"
where id > {lastIdDM} """

    df = pd.DataFrame(query_DBstatisticsByCients(sql_select_all), columns=['id', 'id_client', 'client_name', 'company_token', 'quantity_services_opened',
                                                                           'quantity_message_sended', 'quantity_message_received', 'quantity_looked_by_tag', 'computed_date', 'createdAt'])

    return df
