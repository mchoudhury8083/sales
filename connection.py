import mysql.connector as conn
import pandas as pd
import pymongo
import json
from pandasql import sqldf
import logging

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level= logging.INFO)

def create_tables(cursor):
    try:
        dress_sales = "create table sales.dress_sales(Dress_ID int,  29_8_2013 varchar(50), 31_8_2013 varchar(50)" \
                      ", 2_9_2013 varchar(50),  4_9_2013 varchar(50),  6_9_2013 varchar(50),  8_9_2013 varchar(50)," \
                      "10_9_2013 varchar(50), 12_9_2013 varchar(50), 14_9_2013 varchar(50), 16_9_2013 varchar(50), " \
                      "18_9_2013 varchar(50), 20_9_2013 varchar(50), 22_9_2013 varchar(50), 24_9_2013 varchar(50), " \
                      "26_9_2013 varchar(50), 28_9_2013 varchar(50), 30_9_2013 varchar(50), 2_10_2013 varchar(50), " \
                      "4_10_2013 varchar(50), 6_10_2013 varchar(50), 8_10_2010 varchar(50), 10_10_2013 varchar(50),   " \
                      " 12_10_2013 varchar(50) )  "
        cursor.execute(dress_sales)
    except Exception as e:
        logging.info("Table dress_sales already created",e)

    try:
        dress_dataset ="create table sales.dress_dataset(Dress_ID int,Style varchar(50),Price varchar(50)    ,Rating varchar(50),Size varchar(50),Season varchar(50),NeckLine varchar(50)," \
                       "SleeveLength varchar(50),waiseline varchar(50),Material varchar(50),FabricType varchar(50),Decoration varchar(50),Pattern_Type varchar(50),Recommendation varchar(50))"
        cursor.execute(dress_dataset)
    except Exception as e:
        logging.info("Table dress_dataset already created",e)

def load_data(cursor):
    df = pd.read_excel(r'/Users/rajeshchoudhury/PycharmProjects/mydb/Attribute DataSet.xlsx')
    data_collect = df.to_records()
    #Need to update like below
    for row in data_collect:
        record = "insert into sales.dress_dataset values(" + str(row["Dress_ID"]) + ",'" + row["Style"] + "','" + str(
            row["Price"]) + "'," + str(row["Rating"]) + ",'" + str(row["Size"]) + "','" + str(
            row["Season"]) + "','" + str(row["NeckLine"]) + "" \
                                                            "','" + str(row["SleeveLength"]) + "','" + str(
            row["waiseline"]) + "','" + str(row["Material"]) + "','" + str(row["FabricType"]) + "','" + str(
            row["Decoration"]) + "','" + str(row["Pattern Type"]) + "'," + str(row["Recommendation"]) + ")"
        cursor.execute(record)
        logging.info(record)

    mydb.commit()

    dataset_df = pd.read_excel(r'/Users/rajeshchoudhury/PycharmProjects/mydb/Dress Sales_header_upd.xlsx')
    dataset_collect = dataset_df.to_records()

    for row in dataset_collect:
        sql = (
            "INSERT INTO sales.dress_sales (Dress_ID,   29_8_2013, 31_8_2013, 2_9_2013,  4_9_2013,  6_9_2013,  8_9_2013,  10_9_2013, 12_9_2013," \
            "    14_9_2013, 16_9_2013, 18_9_2013, 20_9_2013, 22_9_2013, 24_9_2013, 26_9_2013, 28_9_2013, 30_9_2013, 2_10_2013, 4_10_2013, 6_10_2013," \
            "    8_10_2010, 10_10_2013,    12_10_2013) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        value = [str(row["Dress_ID"]), str(row["29_8_2013"]), str(row["31_8_2013"]), str(row["2_9_2013"]),
                 str(row["4_9_2013"]), str(row["6_9_2013"]),
                 str(row["8_9_2013"]), str(row["10_9_2013"]), str(row["12_9_2013"]), str(row["14_9_2013"]),
                 str(row["16_9_2013"]), str(row["18_9_2013"]),
                 str(row["20_9_2013"]), str(row["22_9_2013"]), str(row["24_9_2013"]), str(row["26_9_2013"]),
                 str(row["28_9_2013"]), str(row["30_9_2013"]),
                 str(row["2_10_2013"]), str(row["4_10_2013"]), str(row["6_10_2013"]), str(row["8_10_2010"]),
                 str(row["10_10_2013"]), str(row["12_10_2013"])]
        cursor.execute(sql, value)
    mydb.commit()

def store_data_mangodb(json_dataframe):
    client = pymongo.MongoClient(
        "mongodb+srv://jaimaa12:jaiMaata1!@rajeshnosql.zemtbb6.mongodb.net/?retryWrites=true&w=majority")
    dress = client['dress']
    table = dress['dataset']
    json_object = json.loads(json_dataframe)
    table.insert_many(json_object)

#Main code starts here
mydb = conn.connect(host = "127.0.0.1", user = "root", password = "*****")
cursor = mydb.cursor()

#create required tables
#create_tables(cursor)
#load data
#load_data(cursor)

try:
    SQL_Query = pd.read_sql_query(
        '''select Dress_ID,Style,Price,Rating,Size,Season,NeckLine,SleeveLength,waiseline,Material,
        FabricType,Decoration,Pattern_Type,Recommendation	
        from sales.dress_dataset
         ''', mydb)

    df_dataset = pd.DataFrame(SQL_Query, columns=['Dress_ID','Style','Price','Rating','Size','Season',
                                                  'NeckLine','SleeveLength','waiseline','Material','FabricType',
                                                   'Decoration','Pattern_Type','Recommendation'])
    records =df_dataset.to_json(orient='records')
    store_data_mangodb(records)

    SQL_Query_Sales = pd.read_sql_query(
        '''select Dress_ID,29_8_2013,31_8_2013,2_9_2013,4_9_2013,6_9_2013,8_9_2013,10_9_2013,12_9_2013,
            14_9_2013,16_9_2013,18_9_2013,20_9_2013,22_9_2013,24_9_2013,26_9_2013,28_9_2013,30_9_2013,
            2_10_2013,4_10_2013,6_10_2013,8_10_2010,10_10_2013,12_10_2013 
            from sales.dress_sales
         ''', mydb)

    df_sales = pd.DataFrame(SQL_Query_Sales, columns=['Dress_ID','29_8_2013','31_8_2013','2_9_2013','4_9_2013',
                                                      '6_9_2013','8_9_2013','10_9_2013','12_9_2013','14_9_2013',
                                                      '16_9_2013','18_9_2013','20_9_2013','22_9_2013','24_9_2013',
                                                      '26_9_2013','28_9_2013','30_9_2013','2_10_2013','4_10_2013',
                                                      '6_10_2013','8_10_2010','10_10_2013','12_10_2013'])
    #left join
    left_join_df = pd.merge(df_dataset,df_sales, on='Dress_ID',how='left')
    #left_join_answer =left_join_df.to_json(orient="records")
    logging.info(left_join_df)

    #unique
    query = 'SELECT count(distinct(Dress_ID)) FROM df_dataset'
    count = sqldf(query, locals())
    logging.info(count)

    #recommendation  =0 -- Ask this question to Sunny
   # df_recommendation = df_dataset.query("Recommendation='1")
   # df_recommendation_answer = df_recommendation.to_json(orient="records")
   # print(df_recommendation)
    #Need to rewrite using sqldf
    subframe = df_dataset[df_dataset['Recommendation'] == '0']
    logging.info(subframe)

    # Sum of Dress Sales
    sum_query = '''
            select  Dress_ID,SUM(`29_8_2013`)+sum(`31_8_2013`)+sum(`2_9_2013`)+sum(`4_9_2013`)+sum(`6_9_2013`)
            +sum(`8_9_2013`)+sum(`10_9_2013`)+ sum(`12_9_2013`)+sum(`14_9_2013`)+sum(`16_9_2013`)
            +sum(`18_9_2013`)+sum(`20_9_2013`)+sum(`22_9_2013`)+sum(`24_9_2013`)+ sum(`26_9_2013`)
            +sum(`28_9_2013`)+sum(`30_9_2013`)+sum(`2_10_2013`)+sum(`4_10_2013`)+sum(`6_10_2013`)
            +sum(`8_10_2010`)+ sum(`10_10_2013`)+sum(`12_10_2013`) 
            from df_sales
            group by Dress_ID
            '''

    sum_df = sqldf(sum_query, locals())
    logging.info(sum_df)

    third_highest_sales_query = '''
                 select Dress_ID, total_sales from (select  Dress_ID,SUM(`29_8_2013`)+sum(`31_8_2013`)+sum(`2_9_2013`)+sum(`4_9_2013`)+sum(`6_9_2013`)
                +sum(`8_9_2013`)+sum(`10_9_2013`)+ sum(`12_9_2013`)+sum(`14_9_2013`)+sum(`16_9_2013`)
                +sum(`18_9_2013`)+sum(`20_9_2013`)+sum(`22_9_2013`)+sum(`24_9_2013`)+ sum(`26_9_2013`)
                +sum(`28_9_2013`)+sum(`30_9_2013`)+sum(`2_10_2013`)+sum(`4_10_2013`)+sum(`6_10_2013`)
                +sum(`8_10_2010`)+ sum(`10_10_2013`)+sum(`12_10_2013`)  as total_sales 
                from df_sales
                group by Dress_ID order by total_sales desc)  sales LIMIT 2,1
                '''

    third_highest_sales_df = sqldf(third_highest_sales_query, locals())
    logging.info(third_highest_sales_df)
except Exception as e:
    logging.info("Error: unable to convert the data --",e)

mydb.close()


