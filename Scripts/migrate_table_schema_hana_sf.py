#!/usr/bin/env python
# coding: utf-8


#Import requried libraries

import pandas as pd
from hdbcli import dbapi
import snowflake.connector
import datetime
import json


start_time = datetime.datetime.now()
print("Python script started at ",start_time)

def sf_connect(account,user,password,warehouse,database,schema,role):
    sf_conn=snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema = schema,
        role=role
        );
    sf_cur = sf_conn.cursor()
    return(sf_cur)

def hana_connect(address,port,user,password):
    hana_connection = dbapi.connect(address=address, port=port, user=user, password=password)
    return(hana_connection)


with open(r"D:\SAP_SNOWFLAKE\Scripts\Connections\Inbound\data.json") as f:
    json2 = json.load(f)
with open(json2["file_name"]) as fn:
  json4 = json.load(fn)
  hana_conn = hana_connect(json4["address"],json4["port"],json4["user"],json4["password"])
  
def table_display():  
    #sql_cntrl = 'SELECT * from "SAPABAP1"."SF_MIG_OBJ_CTL" '
    sql_cntrl = "SELECT TABNAME FROM SAPABAP1.DD02L WHERE TABCLASS = 'TRANSP' AND CONTFLAG = 'A' AND TABNAME IN (SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = 'SAPABAP1')"
    df = pd.read_sql(sql_cntrl,hana_conn)
    res = df.to_json(orient="split")
    return res

def table_schema_migrate(user_table_names):    
    result = []
    table_list = user_table_names
#   ->  Read json File from API to get table list   
#    table_list=table_list.to_list()
    print("********",table_list)
    if not(table_list): 
        print("Nothing to migrate, Please check table entries")
    else:
      for tname in table_list:
          try:
          
        # with open(r"Connections\Inbound\data.json") as fin:
        #   json8 = json.load(fin)
            schema_name = json4["schema"]
            table_name=str(tname)
            print("??????????? table_name", table_name)
    #        schema_name=tname
    #        sql_hana_tbl = "SELECT (CASE WHEN POSITION > 1 THEN ',' ELSE '' END) || ' ' || '^' || COLUMN_NAME || '^' || ' ' || DATA_TYPE_NAME || (CASE WHEN DATA_TYPE_NAME = NCLOB THEN 'VARCHAR' ELSE '' END) || (CASE WHEN LENGTH IS NULL THEN '' WHEN DATA_TYPE_NAME IN('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT','DOUBLE') THEN '' WHEN SCALE IS NULL THEN '('||LENGTH||')' ELSE '('||LENGTH||','||SCALE||')' END) || (CASE WHEN IS_NULLABLE='FALSE' THEN ' NOT NULL' ELSE '' END) FROM SYS.TABLE_COLUMNS WHERE TABLE_NAME = '" + table_name + "' AND SCHEMA_NAME = '" + schema_name + "' ORDER BY POSITION"
            sql_hana_tbl = "SELECT (CASE WHEN POSITION > 1 THEN ',' ELSE '' END) || ' ' || '^' || COLUMN_NAME || '^' || ' ' || DATA_TYPE_NAME || (CASE WHEN LENGTH IS NULL THEN '' WHEN DATA_TYPE_NAME IN('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT','DOUBLE') THEN '' WHEN SCALE IS NULL THEN '('||LENGTH||')' ELSE '('||LENGTH||','||SCALE||')' END) || (CASE WHEN IS_NULLABLE='FALSE' THEN ' NOT NULL' ELSE '' END) FROM SYS.TABLE_COLUMNS WHERE TABLE_NAME = '" + table_name + "' AND SCHEMA_NAME = '" + schema_name + "' ORDER BY POSITION"
            df1 =pd.read_sql(sql_hana_tbl,hana_conn)
            ddl_text=df1.to_string(index=None)
            dd2_text=ddl_text.replace('^','"')
            
            dd3_text = dd2_text.split("\n",1)[1]
    #        dd3_text="CREATE  " + tname + '"'"(" + dd3_text + ");"
    #        dd3_text="CREATE OR REPLACE TABLE " + tname + "(" + dd3_text + ");"
            dd3_text="CREATE  OR REPLACE TABLE "+'"' + tname +'"'+ "(" + dd3_text + ");"
            dd3_text=dd3_text.replace('\n','').replace('\t','')
            print("DDL Script generated for table " + tname)
            print('\n')
            with open(r"Connections\Outbound\data.json") as snf:
              json3 = json.load(snf)
            with open(json3["file_name"]) as fn1:
              json5 = json.load(fn1) 
            sf_cursor=sf_connect(json5["account"],json5["user"],json5["password"],json5["warehouse"],json5["database"],json5["schema"],json5["role"])        
            sf_cursor.execute(dd3_text)
            tab_res = "Table " + tname + " created in Snowflake"
            result.append(tab_res)
          except Exception:
            pass
#            result.append(tab_res)
#          sql_upd_ctl="UPDATE " + ctrl_tbl + " SET MIG_STAT = 'COMPLETE', UPDT_DT_TM = now() WHERE SCH_NM = '" + schema_name + "' and OBJ_NM = '" + table_name +"'"
#          hana_conn.cursor().execute(sql_upd_ctl)
#          print("Control table updated for: " + table_name)
    return tab_res
    
    #control_table_display()

#table_schema_migrate()
end_time = datetime.datetime.now()
print("The python script completed at ",end_time)
