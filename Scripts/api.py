# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 11:49:15 2021

@author: sowmyag
"""


from flask import Flask,request,jsonify,Response,stream_with_context,redirect,url_for,render_template_string
import json
import pandas as pd
import os
from migrate_table_schema_hana_sf import table_schema_migrate,hana_connect,sf_connect,table_display
from calcmigration import view_migrate
import time
from math import sqrt
import itertools


app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'


@app.route('/snowflake_login/', methods=["GET","POST"])
def sf_schema():    
       json1 = request.get_json()
       print("Snowflake Login",json1)
       outbound= json1["connection_name"]
       with open(r"Connections\\Outbound\\{}.json".format(outbound), 'w') as outbound:
        json.dump(json1, outbound)    
        sf_connect(json1["account"],json1["user"],json1["password"],json1["warehouse"],json1["database"],json1["schema"],json1["role"])
       return "YOU HAVE SUCCESSFULLY LOGGED INTO SNOWFLAKE"

      
@app.route('/Hana_login/', methods=["GET","POST"])
def hf_schema():
     json2 = request.get_json()
     print("***************Hana Login**********",json2)
     inbound= json2["connection_name"]
#     file_name= r"Connections\Inbound\{}.json".format(inbound)
#     json2["file_name"] = file_name
     with open(r"Connections\\Inbound\\{}.json".format(inbound), 'w') as inbound:
      json.dump(json2, inbound)    
     hana_connect(json2["address"],json2["port"],json2["user"],json2["password"])
     return "YOU HAVE SUCCESSFULLY LOGGED INTO HANA DB"
#     return jsonify(json2)
 
@app.route('/table_names/', methods=["GET","POST"])
def tables():
    mig = table_display()
    return jsonify(mig) 

@app.route('/Connection_Inbound/', methods=["GET","POST"])
def conn_inbound():
    in_bnd = [os.path.splitext(i)[0] for i in os.listdir(r"Connections\Inbound")] 
    return jsonify({"inbound_names":in_bnd})

@app.route('/Connection_Outbound/', methods=["GET","POST"])
def conn_Outbound():    
    Out_bnd = [os.path.splitext(i)[0] for i in os.listdir(r"Connections\Outbound")] 
    return jsonify({"Outbound_names":Out_bnd})

@app.route('/tab_namesBasedonConnectionInbound/', methods=["GET","POST"])
def tbl_names():    
    r = request.get_json()
    print("request",r)
    r = list(r.values())  
    print("+++++++++", r)
    In_bnd = [os.path.splitext(i)[0] for i in os.listdir(r"Connections\Inbound")]
    if r[0] in In_bnd:
        file_name =  r"Connections\Inbound" + "\\" + r[0]  + ".json"
        data = {"file_name":file_name}
        with open(r"Connections\Inbound\data.json","w") as fn:
        	json.dump(data,fn)
        with open(file_name) as f:
            j = json.load(f)
            print(j)
        hana_conn = hana_connect(j["address"],j["port"],j["user"],j["password"])
        schema_name = j["schema"]
        sql_cntrl = "SELECT TABNAME FROM SAPABAP1.DD02L WHERE TABCLASS = 'TRANSP' AND CONTFLAG = 'A' AND TABNAME IN (SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = "+ "'"+schema_name+"'" +" AND left(TABNAME,1)!='/') ORDER by TABNAME ASC limit 1000"
#        hana_conn = hana_connect(j["address"],j["port"],j["user"],j["password"])
        df = pd.read_sql(sql_cntrl,hana_conn)
        table_names = df["TABNAME"].to_list()
    return jsonify({"schema_name" : schema_name,"table_names":table_names})

@app.route('/tab_namesBasedonConnectionOutbound/', methods=["GET","POST"])
def tbl_names_Outbound():    
    r = request.get_json()
    r = list(r.values())    
    Out_bnd = [os.path.splitext(i)[0] for i in os.listdir(r"Connections\Outbound")] 
    if r[0] in Out_bnd :
        file_name =  r"Connections\Outbound" + "\\" + r[0]  + ".json"
        data = {"file_name":file_name}
        with open(r"Connections\Outbound\data_snowflake.json","w") as fn:
        	json.dump(data,fn)
        with open(file_name) as f:
            j1 = json.load(f) 
            print(j1)
        sf_cursor = sf_connect(j1["account"],j1["user"],j1["password"],j1["warehouse"],j1["database"],j1["schema"],j1["role"])
        schema_name = j1["schema"]
        sql_cntrl = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA="+ "'"+schema_name+"'" +"  "     
        sf_cursor.execute(sql_cntrl)
        df = sf_cursor.fetch_pandas_all()
        tab_names = df["TABLE_NAME"].to_list()
    return jsonify({"schema_name" : schema_name,"table_names":tab_names})


@app.route('/schema_mig/', methods=["GET", "POST"])
def schema_migration():
    user_table_names = request.get_json()
    user_table_names = list(user_table_names.values())
    user_table_names = user_table_names[0]
    def generate():
        for i in range(len(user_table_names)):
            result = [user_table_names[i]]
            table_result = table_schema_migrate(result)
            yield table_result + '\n'
            # yield ','.join(table_result) + '\n'

    return Response(stream_with_context(generate()))


@app.route('/view_namesBasedonConnectionInbound/', methods=["GET", "POST"])
def view_tbl_names_Inbound():
    r = request.get_json()
    print("request", r)
    r = list(r.values())
    In_bnd = [os.path.splitext(i)[0] for i in os.listdir(r"Connections\Inbound")]
    if r[0] in In_bnd:
        file_name = r"Connections\Inbound" + "\\" + r[0] + ".json"
        data = {"file_name": file_name}
        with open(r"Connections\Inbound\data.json", "w") as fn:
            json.dump(data, fn)
        with open(file_name) as f:
            j = json.load(f)
            #print(j)
        hana_conn = hana_connect(j["address"], j["port"], j["user"], j["password"])
        sql_cntrl = "SELECT TOP 1000 PACKAGE_ID ,OBJECT_NAME FROM _SYS_REPO.ACTIVE_OBJECT WHERE OBJECT_SUFFIX='calculationview' order by PACKAGE_ID"
        #print("sql_cntrl====",sql_cntrl)
        df = pd.read_sql(sql_cntrl, hana_conn)
        data=[]
        for i in range(len(df)):
            d={}
            d['package_id']=df['PACKAGE_ID'][i]
            d['object_name']=df['OBJECT_NAME'][i]
            data.append(d)          
    return jsonify(data)


@app.route('/view_namesBasedonConnectionOutbound/', methods=["GET", "POST"])
def view_tbl_names_Outbound():
    r = request.get_json()
    print("request", r)
    r = list(r.values())
    Out_bnd = [os.path.splitext(i)[0] for i in os.listdir(r"Connections\Outbound")]
    if r[0] in Out_bnd:
        file_name = r"Connections\Outbound" + "\\" + r[0] + ".json"
        data = {"file_name": file_name}
        with open(r"Connections\Outbound\data_snowflake.json", "w") as fn:
            json.dump(data, fn)
        with open(file_name) as f:
            j1 = json.load(f)
            #print(j)
            
        sf_cursor = sf_connect(j1["account"],j1["user"],j1["password"],j1["warehouse"],j1["database"],j1["schema"],j1["role"])
        schema_name = j1["schema"]
        sql_cntrl = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='VIEW' AND TABLE_SCHEMA="+ "'"+schema_name+"'" +"  "
        #print("sql_cntrl====",sql_cntrl)
        sf_cursor.execute(sql_cntrl)
        df = sf_cursor.fetch_pandas_all()
        view_names = df["TABLE_NAME"].to_list()
    return jsonify({"schema_name" : schema_name,"view_names":view_names})

@app.route('/view_mig/', methods=["GET","POST"])
def view_migration():
    payload = request.json
    userinput = payload['userinput']
    view_result ="View Has been Created for "
    for ui in userinput:
        print(ui)
        package_id = ui['package_id']
        user_view_names = ui['user_view_names']
        for i in range(len(user_view_names)):
            res = [user_view_names[i]]
            view_result = view_result + view_migrate(res,package_id) + ","
    return jsonify({"view_result":view_result[:-1]})

   

if __name__ == '__main__':
	
	app.run(debug=False)

