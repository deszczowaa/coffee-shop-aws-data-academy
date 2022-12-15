import json
import numpy as np
import pandas as pd
import awswrangler as wr
import os
import boto3
import io
from datetime import date


boto3.setup_default_session(region_name="eu-west-1")
def query_customer_age():
    query = """  
            SELECT
                COUNT(*) as no_of_customers,
                (d.year - c.birthyear) as age
            FROM sales_reciept sr
            LEFT JOIN customer c using (customer_id)
            LEFT JOIN date d using (date_id)
            GROUP BY 2
            ORDER BY 1 DESC;
            """
    return query

def query_customer_age_groups():
    query = """
    SELECT
        COUNT(*) as no_of_customers,
        CASE
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) < 20 THEN 'UNDER 20'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 20 AND 30 THEN '20-30'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 30 AND 40 THEN '30-40'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 40 AND 50 THEN '40-50'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 50 AND 50 THEN '50-60'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) > 20 THEN 'OVER 60'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) IS NULL THEN 'NO INFO'
        END AS age_groups
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    LEFT JOIN date d using (date_id)
    GROUP BY 2
    ORDER BY 1 DESC;
    """
    return query

def query_trans_with_registered_clients():
    query = """
    SELECT
        (SELECT COUNT(*) FROM sales_reciept sr
            LEFT JOIN customer c using (customer_id) 
            WHERE c.customerfirstname != 'NULL' 
            OR c.customersurname != 'NULL' 
            OR c.loyaltycardnumber != 'NULL' 
            OR c.customeremail != 'NULL') as no_of_transactions_with_registered_customers,
    COUNT(sr.transactionno) as total_no_of_transactions
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    ;
    """
    return query

def query_loyalty_card_holders():
    query = """
    SELECT 
        COUNT(IF(loyaltycardnumber = 'NULL', NULL, 1)) as no_of_loyalty_card_holders,
        COUNT(customerno) AS no_of_registered_customers
    FROM customer;
    """
    return query

def query_transactions_loyalty_card():
    query = """
    SELECT
        COUNT(sr.transactionno) AS no_of_transactions,
        c.loyaltycardnumber AS loyalty_card_number
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    GROUP BY 2
    ORDER BY 1 DESC;
    """
    return query

def query_products_loyalty_card():
    query = """
    SELECT
        SUM(sr.quantity) AS no_of_products,
        c.loyaltycardnumber AS loyalty_card_number
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    GROUP BY 2
    ORDER BY 1 DESC;
    """
    return query

def query_prod_trans_gender():
    query = """
    SELECT
        SUM(sr.quantity) AS no_of_products,
        COUNT(sr.transactionno) AS no_of_transactions,
        c.gender AS gender
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    GROUP BY 3;
    """
    return query
    
def query_prod_trans_age_groups():
    query = """
    SELECT
        COUNT(*) no_of_customers,
        CASE
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) < 20 THEN 'UNDER 20'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 20 AND 30 THEN '20-30'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 30 AND 40 THEN '30-40'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 40 AND 50 THEN '40-50'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 50 AND 50 THEN '50-60'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) > 20 THEN 'OVER 60'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) IS NULL THEN 'NO INFO'
        END AS age_groups,
        SUM(sr.quantity) AS no_of_products,
        COUNT(sr.transactionno) AS no_of_transactions
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    LEFT JOIN date d using (date_id)
    GROUP BY 2;
    """
    return query


def query_prod_trans_gender_cat():
    query = """
    SELECT
        SUM(sr.quantity) AS no_of_products,
        COUNT(sr.transactionno) AS no_of_transactions,
        p.productcategory AS product_category,
        c.gender AS gender
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    LEFT JOIN product p using(product_id)
    GROUP BY  3, 4;
    """
    return query

def query_prod_trans_age_g_cat():
    query = """
    SELECT
        CASE
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) < 20 THEN 'UNDER 20'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 20 AND 30 THEN '20-30'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 30 AND 40 THEN '30-40'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 40 AND 50 THEN '40-50'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) BETWEEN 50 AND 50 THEN '50-60'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) > 20 THEN 'OVER 60'
            WHEN (cast(d.year as int) - cast(c.birthyear as int)) IS NULL THEN 'NO INFO'
        END AS age_groups,
        SUM(sr.quantity) AS no_of_products,
        COUNT(sr.transactionno) AS no_of_transactions,
        p.productcategory AS product_category
    FROM sales_reciept sr
    LEFT JOIN customer c using (customer_id)
    LEFT JOIN date d using (date_id)
    LEFT JOIN product p using (product_id)
    GROUP BY 1, 4;
    """
    return query


def get_dataframe(query):
    df = wr.athena.read_sql_query(query, database="db-conform-coffee-shop")
    return df

def get_all_data():
    dataframes = {}
    customer_age = get_dataframe(query_customer_age())
    customer_age_groups = get_dataframe(query_customer_age_groups())
    trans_with_registered_clients = get_dataframe(query_trans_with_registered_clients())
    loyalty_card_holders = get_dataframe(query_loyalty_card_holders())
    transactions_loyalty_card = get_dataframe(query_transactions_loyalty_card())
    products_loyalty_card = get_dataframe(query_products_loyalty_card())
    prod_trans_gender = get_dataframe(query_prod_trans_gender())
    prod_trans_age_groups = get_dataframe(query_prod_trans_age_groups())
    prod_trans_gender_cat = get_dataframe(query_prod_trans_gender_cat())
    prod_trans_age_g_cat = get_dataframe(query_prod_trans_age_g_cat())
    
    dataframes['customer_age'] = customer_age
    dataframes['customer_age_groups'] = customer_age_groups
    dataframes['trans_with_registered_clients'] = trans_with_registered_clients
    dataframes['loyalty_card_holders'] = loyalty_card_holders
    dataframes['transactions_loyalty_card'] = transactions_loyalty_card
    dataframes['products_loyalty_card'] = products_loyalty_card
    dataframes['prod_trans_gender'] = prod_trans_gender
    dataframes['query_prod_trans_age_groups'] = prod_trans_age_groups
    dataframes['prod_trans_gender_cat'] = prod_trans_gender_cat
    dataframes['prod_trans_age_g_cat'] = prod_trans_age_g_cat
    
    return dataframes

def write_to_excel_file(dataframes, bucket_name, filename):
    today = date.today()
    d1 = today.strftime("%d-%m-%Y")
    filename = 'reports' + d1 + filename
    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for key in dataframes:
                dataframes[key].to_excel(writer, sheet_name=key, index=False)
        data = output.getvalue()

    s3 = boto3.resource('s3')
    s3.Bucket(bucket_name).put_object(Key=filename, Body=data)

def lambda_handler(event, context):
    bucket = event['Bucket']
    filename = event['Filename']
    dataframes = get_all_data()
    write_to_excel_file(dataframes, bucket, filename)