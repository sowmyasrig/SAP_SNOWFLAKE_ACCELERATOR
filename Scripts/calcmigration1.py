import json,xmltodict
from hdbcli import dbapi
import pandas as pd
import snowflake.connector


Alljoinqueries, sourcecolumns,targetcolumns,temp= {}, {}, {}, {}
collisttarget = {}
collistSource = {}

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



def Projection(dict, prj):
    listcolnmest, strcolnmest, listcolnmess, strcolnmess,TrgtSorc_dict = {},{}, {}, {}, {}
    TABLE_NAMES, joinattr = [], []
    for j in dict['Calculation:scenario']['calculationViews']['calculationView']:
        if j['@id'] == prj:
            TrgtSorc_dict = {}
            colnamest = []
            strnt = ''
            colnamess = []
            strns = ''
            calc = ''
            condition = ''
            ifqury = ''
            try:
                if j["calculatedViewAttributes"] is not None:
                    for c in j["calculatedViewAttributes"]["calculatedViewAttribute"]:
                        calcid = c["@id"]
                        calc = c["formula"]
                        # print("..............")
                        try:
                            if "if" in calc or "IF" in calc:
                                formula = calc[3:-1]
                                CalCondition = formula.split(",")
                                ifqury =ifqury + "case when " + CalCondition[0] + " then " + CalCondition[1] + " else " + \
                                         CalCondition[2] + " end as "  + '"'+ calcid +'"' + ","
                                # print(ifqury)
                            if "midstr" in calc:
                                calcmid = calc.replace("midstr", "substr")
                                condition =condition+calcmid + " as " + '"' + calcid + '"'+","
                                # print(condition[:-1])
                        except Exception as e:
                            print(e)
                else:
                    pass
            except:
                pass
            try:
                tablename = j['input']['@node'][1:]
                for k in j['input']['mapping']:
                    colnamest.append(k['@target'])
                    colnamess.append(k['@source'])
                    TrgtSorc_dict[k['@source']] = k['@target']
                    if k['@source'] != k['@target']:
                        temp[k['@target']]=k['@source']
                    listcolnmest[tablename] = colnamest
                    strnt = ','.join(colnamest)
                    strcolnmest[tablename] = strnt
                    listcolnmess[tablename] = colnamess
                    strns = ','.join(colnamess)
                    strcolnmess[tablename] = strns
                    TABLE_NAMES.append(tablename)
                sourcecolumns[prj] = colnamess
                targetcolumns[prj] = colnamest
                if 'joinAttribute' in j:
                    for g in j['joinAttribute']:
                        joinattr.append(g['@name'])
                Alljoinqueries[prj] = query(TrgtSorc_dict, prj, tablename,calc,condition,ifqury,dict)
            except:
                pass

def query(TrgtSorc_dict, prj, tablename,calc,condition,ifqury,dict):
    query, selquery,filterqury = '', '',''
    Ts = list(TrgtSorc_dict.keys())
    for l in dict['Calculation:scenario']['calculationViews']['calculationView']:
        if l['@id']==prj:
            count = 0
            for f in l['viewAttributes']['viewAttribute']:
                if "filter" in f:
                    count=count+1
                    filterid = f['@id']
                    filtervalue = f['filter']['@value']
                    if count==1:
                        filterqury = filterqury +" Where "+ filterid + " = " + "'" + filtervalue + "'" +" and "
                    else:
                        filterqury = filterqury + filterid + " = " + "'" + filtervalue + "'" + " and "
    if calc=='':
        for j in range(0, len(Ts)):
            if j == len(Ts) - 1:
                selquery = selquery + '"'+Ts[j] +'"'+ " as " +'"'+ TrgtSorc_dict[Ts[j]]+'"'
            else:
                selquery = selquery +'"'+ Ts[j]+'"'+ " as " +'"'+ TrgtSorc_dict[Ts[j]] +'"'+ ","
        query = prj + " as " + "(" + "select " + selquery + " from " + tablename +filterqury[:-5]  + ")"
        #print(query)
    elif "midstr" in calc :
        for j in range(0, len(Ts)):
            if j == len(Ts) - 1:
                selquery = selquery +condition + '"'+Ts[j] +'"'+ " as " +'"'+ TrgtSorc_dict[Ts[j]]+'"'
            else:
                selquery = selquery +'"'+ Ts[j]+'"'+ " as " +'"'+ TrgtSorc_dict[Ts[j]] +'"'+ ","
        query = prj + " as " + "(" + "select " + selquery + " from " + tablename +filterqury[:-5]  + ")"
        #print(query)
    elif "if" in calc or "IF" in calc:
        for j in range(0, len(Ts)):
            if j == len(Ts) - 1:
                selquery = selquery + ifqury +condition + '"'+Ts[j] +'"'+ " as " +'"'+ TrgtSorc_dict[Ts[j]]+'"'
            else:
                selquery = selquery +'"'+ Ts[j]+'"'+ " as " +'"'+ TrgtSorc_dict[Ts[j]] +'"'+ ","
        query = prj + " as " + "(" + "select " + selquery + " from " + tablename +filterqury[:-5] + ")"
        #print(query)
        
    return query

def Aggregationdef(dict, agg):
    for j in dict['Calculation:scenario']['calculationViews']['calculationView']:
        if j['@id'] == agg:
            Aggquery = ""
            prjmbrs=""
            AggCondlist = []
            Aggcolist = []
            projection = j['input']['@node'][1:]
            Attributes = j['viewAttributes']['viewAttribute']
            for ji in j["input"]["mapping"]:
                prjmbrs=prjmbrs + '"'+ji["@source"]+ '"'+" as "+ '"'+ ji["@target"]+ '"'+","
            for k in range(0, len(Attributes)):
                Aggcols = Attributes[k]['@id']
                Aggcolist.append(Aggcols)
                if '@aggregationType' in Attributes[k]:
                    Aggtype = Attributes[k]['@aggregationType']
                    Aggid = Attributes[k]['@id']
                    AggCondlist.append(Aggid)
                    if k != len(Attributes) - 1:
                        Aggquery = Aggquery + Aggtype + "(" + '"'+Aggid +'"'+ ")" + " as " + '"'+Aggid + '"'+","
                    else:
                        Aggquery = Aggquery + Aggtype + "(" +'"'+ Aggid +'"'+ ")" + " as " + '"'+Aggid+'"'
                else:
                    Aggid = Attributes[k]['@id']
                    if k != len(Attributes) - 1:
                        Aggquery = Aggquery + '"'+Aggid +'"'+ ","
                    else:
                        Aggquery = Aggquery +'"'+ Aggid +'"'
    Grpbylist = list(set(Aggcolist) - set(AggCondlist))
    Grpby = str("(" + (',').join('"' + item + '"' for item in Grpbylist) + ")")
    Projection_ag=projection+"_ag"+" as ( select "+prjmbrs[:-1]+ " from "+projection+" )"
    Alljoinqueries[projection+"_ag"]=Projection_ag
    Aggquery = agg + " as " + "( select " + Aggquery + " from " + projection + "_ag" + " group by " + Grpby + ")"
    Alljoinqueries[agg] = Aggquery

def joinquery(dict, d):
    colnamest=[]
    colnamess=[]
    selectattr=""
    jointargetcolumns={}
    joinsourcecolumns={}
    joinatt= []
    tables = []
    jointrgtlst, joinsrclst={},{}
    for j in dict['Calculation:scenario']['calculationViews']['calculationView']:
        if j['@id'] == d:
            jointype = j["@joinType"]
            if jointype == "leftOuter" or jointype == "rightOuter":
                b = jointype[:-5]
                c = jointype[4:]
                jointype = b + " " + c
            for i in range(0,len(j["input"])):
               tables.append(j["input"][i]["@node"][1:])
            if "joinAttribute" in j:
                if len(j["joinAttribute"])!=1:
                    for n in j["joinAttribute"]:
                        joinatt.append(n["@name"])
                else:
                    joinatt.append(j["joinAttribute"]["@name"])
            try:
                for inp in j["input"]:
                    projtarget,projsource=[],[]
                    prstrnt,prstrns="",""
                    tablename=inp["@node"][1:]
                    for m in inp["mapping"]:
                        projtarget.append(m["@target"])
                        projsource.append(m["@source"])
                        jointrgtlst[tablename]=projtarget
                        joinsrclst[tablename]=projsource
            except:
                pass
    for j in dict['Calculation:scenario']['calculationViews']['calculationView']:
        joinquery=""
        if j["@id"]==d:
            selectstmt = ""
            for v in j["viewAttributes"]["viewAttribute"]:
                if v["@id"] in joinatt:
                    selectstmt = selectstmt+tables[0]+"_jn."+'"' +v["@id"] + '"' + ","
                else:
                    selectstmt=selectstmt+'"'+v["@id"]+'"'+","
        else:
            continue
    attquery=""
    for ja in joinatt:
        attquery=attquery+tables[0]+"_jn."+'"'+ja+'"'+"="+tables[1]+"_jn."+'"'+ja+'"'+" and "
    for i in tables:
        query = ""
        for trt in range(0,len(jointrgtlst[i])):
             query=query+'"'+joinsrclst[i][trt]+'"'+" as "+'"'+jointrgtlst[i][trt]+'"'+","
        joinquery=i+"_jn"+" as (select "+ query[:-1] + " from "+i+")"
        Alljoinqueries[i+"_jn"]=joinquery
    if joinquery != "":
        finaljoin = d + " as (select " + selectstmt[:-1] + " from " + tables[0] + "_jn" + " " + jointype + " join " + tables[1] + "_jn" + " on " + attquery[:-4] + ")"
        Alljoinqueries[d] = finaljoin
    return finaljoin
def parsing_xml(file):
    data_dict = xmltodict.parse(file)
    json_data = json.dumps(data_dict)
    dict = json.loads(json_data)
    viewname=dict["Calculation:scenario"]["@id"]
    # creating list of table names
    DATA_TABLE_NAMES = []
    global DATA_TABLE_NAMES_type
    DATA_TABLE_NAMES_type = []
    for i in dict['Calculation:scenario']['dataSources']['DataSource']:
        DATA_TABLE_NAMES.append(i['@id'])
        if i['@type'] == "CALCULATION_VIEW":
            DATA_TABLE_NAMES_type.append(i['@id'])
    TABLE_type_List = []
    for j in dict['Calculation:scenario']['calculationViews']['calculationView']:
        TABLE_type_List.append(j['@id'])
    for d in range(0,len(TABLE_type_List)):
        if d != len(TABLE_type_List) - 1:
            # print("small query")
            if TABLE_type_List[d][0:10] == "Projection":
                Projection(dict, TABLE_type_List[d])
            elif TABLE_type_List[d][:11] == "Aggregation":
                Aggregationdef(dict, TABLE_type_List[d])
            elif TABLE_type_List[d][:4] == "Join":
                joinquery(dict, TABLE_type_List[d])
        else:
            joinquery(dict, TABLE_type_List[d])
            executionquery = ''
            for i in Alljoinqueries.keys():
                executionquery = executionquery + Alljoinqueries[i] + ","
            finalquery="Create or replace view "+viewname+" as with " + executionquery[:-1]+"select * from "+TABLE_type_List[d]
            Alljoinqueries.clear()
            with open(r"Connections\Outbound\data.json") as snf:
                    json3 = json.load(snf)
            with open(json3["file_name"]) as fn1:
                    json5 = json.load(fn1) 
            sf_cursor=sf_connect(json5["account"],json5["user"],json5["password"],json5["warehouse"],json5["database"],json5["schema"],json5["role"]) 
            #print(finalquery)
            sf_cursor.execute("USE WAREHOUSE HANA_SF_POC")
            sf_cursor.execute("USE DATABASE HANA_DB")
            sf_cursor.execute("use SCHEMA SAPABAP1")
            sf_cursor.execute(finalquery)
            
            #print("View created")

def view_migrate(user_view_names):
    with open(r"D:\SAP_SNOWFLAKE\Scripts\Connections\Inbound\data.json") as f:
            json2 = json.load(f)
    with open(json2["file_name"]) as fn:
          json4 = json.load(fn)
    hana_conn = hana_connect(json4["address"],json4["port"],json4["user"],json4["password"])
    view_list = user_view_names
    print("********",view_list)
    try:
        for i in view_list:
            view_name=str(i)
            print("??????????? view_name", view_name)
            if i == "PO_GR_IR_QUERY":
                tab1 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" + i + "'"
                df1 = pd.read_sql(tab1, hana_conn)
                df2 = df1.at[0,'CDATA']
                if df2.find("Projection_23$$$$EKBE$$"):
                    df2 = df2.replace("Projection_23$$$$EKBE$$", "EKBE")
                    parsing_xml(df2)
                    res = "View has been created for " + i
                    return i
            else:
                tab2 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" + i + "'"
                df3 = pd.read_sql(tab2, hana_conn)
                try:
                    parsing_xml(df3.at[0,'CDATA'])
                    res = "View has been created for " + i
                except:
                   tab1 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" +  DATA_TABLE_NAMES_type[0] + "'"
                   df4 = pd.read_sql(tab1, hana_conn)
                   parsing_xml(df4.at[0,'CDATA'])
                   tab1 = "SELECT CDATA FROM _SYS_REPO.ACTIVE_OBJECT WHERE PACKAGE_ID = PACKAGE_ID AND OBJECT_NAME = " + "'" + i + "'"
                   df1 = pd.read_sql(tab1, hana_conn)
                   parsing_xml(df1.at[0,'CDATA'])
                   res = "View has been created for " + i
                return i
    except:
        pass
        

    