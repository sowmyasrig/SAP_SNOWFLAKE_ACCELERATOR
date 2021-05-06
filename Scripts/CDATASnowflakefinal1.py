import json
import snowflake.connector as sf
import xmltodict
from hdbcli import dbapi
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from param import *

def sf_connect():
    url = URL(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema =schema,
    role=role
    )
    engine = create_engine(url)
    connection = engine.connect()
    return(connection)
cs=sf_connect()

# '''def Connection():
#     conn = sf.connect(
#         user='SF_POC_USER',
#         password='MT1234',
#         account='randomtreespartner.east-us-2.azure',
#         database='HANA_DB',
#         schema='SAPABAP1')
#     cs = conn.cursor()
#     return cs '''

def parsing_xml(file):
    # laoding file and conversions
    #with open(file) as xml_file:
    data_dict = xmltodict.parse(file)
    json_data = json.dumps(data_dict)
    dict = json.loads(json_data)
    #with open(r"D:\snowflake\xml\formula.json", "w") as json_file:
        #json_file.write(json_data)
    viewname = dict['Calculation:scenario']['@id']
    # creating list of table names
    DATA_TABLE_NAMES = []
    DATA_TABLE_NAMES_type = []
    for i in dict['Calculation:scenario']['dataSources']['DataSource']:
        DATA_TABLE_NAMES.append(i['@id'])
        if i['@type'] == "CALCULATION_VIEW":
            DATA_TABLE_NAMES_type.append(i['@id'])
    # creating dict of table col names
    TABLE_COLUMNS = {}
    TABLE_COLUMNS_IND = {}
    LeftTableclm = []
    str = ''
    re = '' 
    filtercol =''
    filterval = ''
    labels_dict ={}
    CalCondition = []
    derviedcol = ''
    jointype=''
    re1 = ''
    try:
        labels = dict['Calculation:scenario']['logicalModel']
        for k in labels['attributes']['attribute']:
            labels_dict[k['@id']]=k['descriptions']['@defaultDescription']
        m_id = labels['baseMeasures']['measure']['@id']
        m_col = labels['baseMeasures']['measure']['descriptions']['@defaultDescription']
        labels_dict[m_id]=m_col
    except:
        pass
    if "Union_1" in json_data:
        Unionjoin = labels['@id']
        jointype = Unionjoin.split("_")[0]
    else:
        for i in dict['Calculation:scenario']:
            if i == 'calculationViews':
                a = dict['Calculation:scenario'][i]['calculationView']
                n = len(a)-1
                x = a[n]['@joinType']
                try:
                    for j in a[n]['joinAttribute']:
                         LeftTableclm.append(j['@name'])
                except:
                    pass
                if x == "leftOuter"or x == "rightOuter":
                    b = x[:-5]
                    c = x[4:]
                    jointype = b+" "+c
                elif x=="inner":
                    jointype = a[n]['@joinType']
                if "formula" in json_data:
                        derviedcol = a[n]['calculatedViewAttributes']['calculatedViewAttribute']['@id']
                        calc =  a[n]['calculatedViewAttributes']['calculatedViewAttribute']['formula']
                        formula = calc[3:-1]
                        CalCondition = formula.split(",")
                try:
                    for i in range(n):
                        i = a[i]
                        re = i['input']['@node']
                        if (re.split('#'))[1] in DATA_TABLE_NAMES:
                            b = i['input']['mapping']
                            col_name = []
                            for k in b:
                                col_name.append(k['@target'])
                            strn = ', '.join(col_name)
                            TABLE_COLUMNS[(re.split('#'))[1]] = strn
                            TABLE_COLUMNS_IND[(re.split('#'))[1]] = col_name      
    
                    i = a[n]
                    re = i['joinAttribute']['@name']
                    re1 = len(i['joinAttribute'])
                except:
                    pass
                try:
                    for k in a:
                        for f in k['viewAttributes']['viewAttribute']:
                            if "filter" in f:
                                filterval = f['filter']['@value']
                                filtercol = f['@id']
                                
                except:
                    pass
        
    return TABLE_COLUMNS,TABLE_COLUMNS_IND,jointype,LeftTableclm,viewname,labels_dict,DATA_TABLE_NAMES,filterval,filtercol,json_data,dict,CalCondition,derviedcol,re,re1,DATA_TABLE_NAMES_type
def creating_view_join(TABLE_COLUMNS,dict):
    # creating view for individual table
    join_columns =[]
    tables = []
    newSet = set()
    joinquery=""
    query=""
    finalquery = ''
    if jointype!="Union":
        for table, columns in TABLE_COLUMNS.items():
            join_columns.append(columns)
            tables.append(table)
        setA = set(join_columns[0])
        setB = set(join_columns[1])
        setC = setA&setB
        setA  = setA - setC
        setB = setB - setC
        finalList = []
        for item in setA:
            finalList.append(tables[0]+"."+item)
        for item in setB:
            finalList.append(tables[1] + "." + item)
        for item in setC:
            finalList.append(tables[0] + "." + item)
        selected_columns = (',').join(finalList)
        if re1!=1:
            for i in range(0,len(LeftTableclm)):
                if i == len(LeftTableclm) - 1:
                    joinquery = joinquery + tables[0] + "." + LeftTableclm[i] + " = " + tables[1] + "." + LeftTableclm[i]
                else:
                    joinquery = joinquery + tables[0] + "." + LeftTableclm[i] + " = " + tables[1] + "." + LeftTableclm[i] + " and "
        elif re1==1:
             joinquery = tables[0] + "." + re + "=" + tables[1] + "." + re
        x = list(labels_dict.keys())
        for i in range(0, len(list(labels_dict.keys()))):
            y = x[i]
            z = labels_dict[y]
            if i == len(x) - 1:
                if y in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[0]]:
                    query = query + DATA_TABLE_NAMES[0] + "." + y + " as " + z
                elif y in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[1]]:
                    query = query + DATA_TABLE_NAMES[1] + "." + y + " as " + z
            else:
                if y in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[0]]:
                    query = query + DATA_TABLE_NAMES[0] + "." + y + " as " + z + ","
                elif y in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[1]]:
                    query = query + DATA_TABLE_NAMES[1] + "." + y + " as " + z + ","    
        if "filter" not in json_data:
             if "formula" in json_data:
                 finalquery = "create or replace view " + viewname  + " as select " + query + "," + "case when " + CalCondition[0] + " then " +CalCondition[1] + " else " + CalCondition[2] + " end as " + derviedcol + " from " + tables[0] + " " + jointype + " join " + tables[1] + " on " + joinquery
             else:
                finalquery = "create or replace view " + viewname  + " as select " + query + " from " + tables[0] + " " + jointype + " join " + tables[1] + " on " + joinquery
            #finalquery = "create or replace view "+viewname+"_view "+"as select "+query+" from "+ tables[0]+" "+ jointype+" join "+tables[1]+" on "+joinquery
        else:
            if "formula" in json_data:
                if filtercol in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[0]]:
                    finalquery = "create or replace view " + viewname + " as select " + query + "," + "case when " + CalCondition[0] + " then " +CalCondition[1] + " else " +CalCondition[2] + " end as " + derviedcol + " from " + tables[0] + " " + jointype + " join " + tables[1] + " on " + joinquery + " where " + tables[0] + "." + filtercol + " = " + "'" + filterval + "'"
                elif filtercol in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[1]]:
                    finalquery = "create or replace view " + viewname  + " as select " + query + "," + "case when " + CalCondition[0] + " then " +CalCondition[1] + " else " +CalCondition[2] + " end as " + derviedcol + " from " + tables[0] + " " + jointype + " join " + tables[1] + " on " + joinquery + " where " + tables[1] + "." + filtercol + " = " + "'" + filterval + "'"
            else:
                if filtercol in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[0]]:
                    finalquery = "create or replace view " + viewname  + " as select " + query + " from " + tables[0] + " " + jointype + " join " + tables[1] + " on " + joinquery + " where " + tables[0] + "." + filtercol + " = " + "'" + filterval + "'"
                elif filtercol in TABLE_COLUMNS_IND[DATA_TABLE_NAMES[1]]:
                    finalquery = "create or replace view " + viewname + " as select " + query + " from " + tables[0] + " " + jointype + " join " +tables[1] + " on " + joinquery + " where " + tables[1] + "." + filtercol + " = " + "'" + filterval + "'"
        #print(finalquery)
        cs = sf_connect()
        cs.execute("USE WAREHOUSE HANA_SF_POC")
        cs.execute("USE DATABASE HANA_DB")
        cs.execute("use SCHEMA SAPABAP1")
        cs.execute(finalquery)
    elif jointype == "Union":
        Unionset = set()
        colnme = {}
        i = 0
        for z in dict['Calculation:scenario']:
            if z == 'calculationViews':
                a = dict['Calculation:scenario'][z]['calculationView']
                n = len(a)-1
                x = a[n]['input']
                for k in x:
                    unionrow = k['@emptyUnionBehavior']
                    if unionrow == "NO_ROW":
                        tgtname = []
                        tgtname_null = []
                        tgtname_final = []
                        try:
                            for g in k['mapping']:
                                #x = g['@target']
                                Unionset.add(g['@target'])
                                if '@null' in g:
                                    tgtname.append(g['@target'])
                                else:
                                    tgtname_null.append(g['@target'])
                            tgtname_final.append(tgtname)
                            tgtname_final.append(tgtname_null)
                            colnme[DATA_TABLE_NAMES[i]] = tgtname_final
                            i = i + 1
                            
                        except:
                            pass
                                
                finallist = list(Unionset)
                uquery = ""
                uquery1 = ""
                for i in range(0, len(finallist)):
                    if finallist[i] in colnme[DATA_TABLE_NAMES[0]][1]:
                        if i == len(finallist) - 1:
                            uquery = uquery + finallist[i]
                        else:
                            uquery = uquery + finallist[i] + ","
                    elif finallist[i] in colnme[DATA_TABLE_NAMES[0]][0]:
                        if i == len(finallist) - 1:
                            uquery = uquery + " Null as " + finallist[i]
                        else:
                            uquery = uquery + " Null as " + finallist[i] + ","
                for i in range(0, len(finallist)):
                    if finallist[i] in colnme[DATA_TABLE_NAMES[1]][1]:
                        if i == len(finallist) - 1:
                            uquery1 = uquery1 + finallist[i]
                        else:
                            uquery1 = uquery1 + finallist[i] + ","
                    elif finallist[i] in colnme[DATA_TABLE_NAMES[1]][0]:
                        if i == len(finallist) - 1:
                            uquery1 = uquery1 + " Null as " + finallist[i]
                        else:
                            uquery1 = uquery1 + " Null as " + finallist[i] + ","
                finalquery = "create or replace view " + viewname  + " as select " + uquery + " from " + DATA_TABLE_NAMES[0] + " " + jointype + " select " + uquery1 + " from " + DATA_TABLE_NAMES[1]
                #print(finalquery)
                cs = sf_connect()
                cs.execute("USE WAREHOUSE HANA_SF_POC")
                cs.execute("USE DATABASE HANA_DB")
                cs.execute("use SCHEMA SAPABAP1")
                cs.execute(finalquery)
                
                
        
if __name__ == '__main__':
    conn = dbapi.connect(address="172.16.40.42", port=30015, user="sgodavari", password="Mouri$14")
    print("Connected to HANA System")
    #tab = "SELECT * FROM SAPABAP1.SF_MIG_OBJ_CTL WHERE SCHEMA_NAME = SCHEMA_NAME AND OBJECT_TYPE = 'VIEW' and MIGRATE_FLAG = 'Y'"
    tab = "SELECT SCH_NM AS SAPABAP1, OBJ_NM AS VIEW, MIG_FLG AS Y, MIG_STAT AS Y, TO_VARCHAR(UPDT_DT_TM, 'YYYY-MM-DD HH:MM:SS') AS UPDATED_AT FROM " + "SAPABAP1.SF_MIG_OBJ_CTL" + " WHERE OBJ_TYP='VIEW'"
    df = pd.read_sql(tab, conn)
    print(df)
    viewnmelst = df['VIEW'].to_list()
    #sprint(viewnmelst)
    
    for i in viewnmelst:
        try:
            tab1 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" + i + "'"
            df1 = pd.read_sql(tab1, conn)
            TABLE_COLUMNS,TABLE_COLUMNS_IND,jointype,LeftTableclm,viewname,labels_dict,DATA_TABLE_NAMES,filterval,filtercol,json_data,dict,CalCondition,derviedcol,re,re1,DATA_TABLE_NAMES_type = parsing_xml(df1.at[0,'CDATA'])
            creating_view_join(TABLE_COLUMNS_IND,dict)
            print("View has been created for ",i)
        except:
             tab1 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" + DATA_TABLE_NAMES_type[0] + "'"
             df1 = pd.read_sql(tab1, conn)
             TABLE_COLUMNS,TABLE_COLUMNS_IND,jointype,LeftTableclm,viewname,labels_dict,DATA_TABLE_NAMES,filterval,filtercol,json_data,dict,CalCondition,derviedcol,re,re1,DATA_TABLE_NAMES_type = parsing_xml(df1.at[0,'CDATA'])
             creating_view_join(TABLE_COLUMNS_IND,dict)
             #print("innerjoin")
             tab1 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" + i + "'"
             df1 = pd.read_sql(tab1, conn)
             TABLE_COLUMNS,TABLE_COLUMNS_IND,jointype,LeftTableclm,viewname,labels_dict,DATA_TABLE_NAMES,filterval,filtercol,json_data,dict,CalCondition,derviedcol,re,re1,DATA_TABLE_NAMES_type = parsing_xml(df1.at[0,'CDATA'])
             creating_view_join(TABLE_COLUMNS_IND,dict)
             print("View has been created for ",i)
             #print("totalview")
            
        

