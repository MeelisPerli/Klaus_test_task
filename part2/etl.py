import json
import re
import pandas as pd
from pandas_gbq import to_gbq


def extract_data(file_path: str, offset: list) -> dict:
    """
    Load data from a json file
    :param file_path: path to the json file
    :return: a python dictionary
    """
    # can't really use offset here. I'd assume that the data is from an API or database that can be queried with an offset

    with open(file_path, 'r') as file:
        # replace single quotes with double quotes
        json_str = file.read()

        json_str = json_str.replace("'", '"')
        json_str = json_str.replace("True", '"True"')
        json_str = json_str.replace("False", '"False"')
        # Replace double quotes with single quotes within square brackets
        json_str = re.sub(r'\[(.*?)\]', lambda x: x.group(0).replace('"', "'"), json_str)
        return json.loads(json_str)


def extract_table_from_table(df, table_column_name, id_column_name, base_table_name):
    """
    Extract a table from a table
    :param df: a pandas dataframe
    :param table_column_name: the column name to extract
    :param id_column_name: the column name to use as the id
    :return: a pandas dataframe
    """
    new_df = df[[id_column_name, table_column_name]]
    new_df = new_df.explode(table_column_name)
    new_df = pd.concat([new_df.drop([table_column_name], axis=1),
                        new_df[table_column_name].apply(pd.Series)], axis=1)
    new_df.rename(columns={id_column_name: base_table_name + '_id'}, inplace=True)

    return new_df


def handle_subscription_data(df: pd.DataFrame):
    """
    Handle the subscription data
    :param df: a list of dictionaries
    :return: a pandas dataframe
    """
    # turn subscription_items, item_tiers, coupons into a separate dataframe
    subscription_items_df = extract_table_from_table(df, 'subscription_items', 'id', 'subscription')
    item_tiers_df = extract_table_from_table(df, 'item_tiers', 'id', 'subscription')
    coupons_df = extract_table_from_table(df, 'coupons', 'id', 'subscription')
    # remove the columns that were extracted
    df.drop(['subscription_items', 'item_tiers', 'coupons'], axis=1, inplace=True)

    # rename id column to subscription_id
    df.rename(columns={'id': 'subscription_id'}, inplace=True)
    return {"subscription": df, "subscription_items": subscription_items_df, "item_tiers": item_tiers_df,
            "coupons": coupons_df}


def handle_customer_data(df: pd.DataFrame):
    """
    Handle the customer data
    :param df: a list of dictionaries
    :return: a pandas dataframe
    """

    # flatten billing_address column, keep id
    billing_address_df = df[['id', 'billing_address']].copy()
    billing_address_df = pd.concat([billing_address_df.drop(['billing_address'], axis=1),
                                    billing_address_df['billing_address'].apply(pd.Series)], axis=1)

    # rename id column to billing_address_id
    billing_address_df.rename(columns={'id': 'customer_id'}, inplace=True)

    # remove billing_address column
    df.drop(['billing_address'], axis=1, inplace=True)

    # tax_providers_fields should probably also be made into a table, but it is empty so I will remove it for now
    df.drop(['tax_providers_fields'], axis=1, inplace=True)

    # rename id column to customer_id
    df.rename(columns={'id': 'customer_id'}, inplace=True)

    return {"customer": df, "billing_address": billing_address_df}


def transform_data(data: dict):
    """
    Transform the json data into a pandas dataframe. Additional transformations can be done here as well.
    :param data: a python dictionary
    :return: a pandas dataframes
    """
    # Transform the data into a pandas dataframe
    print(data.keys())
    # offset can be used for incremental updates
    offset = data['next_offset']
    # data is split into subscription and customer data
    subscription_df = pd.DataFrame([d['subscription'] for d in data['list']])
    customer_df = pd.DataFrame([d['customer'] for d in data['list']])

    result = {}
    result.update(handle_subscription_data(subscription_df))
    result.update(handle_customer_data(customer_df))
    return result, offset


def load_data(df_dict):
    """
    Load the data into the bigquery database
    :param df_dict: a dictionary of pandas dataframes
    :return: None
    """
    for table_name, df in df_dict.items():
        to_gbq(df, "klaus." + table_name, project_id='test-project-430810', if_exists='append')


def main():
    offset = -1
    while True:
        data = extract_data("data/etl.json", offset)
        df_dict, offset = transform_data(data)
        load_data(df_dict)
        if offset == -1:
            break

# To make it into an Airflow DAG, you can create a DAG object and add the main function as a task
# For example
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
#
# with DAG('etl_dag', start_date=datetime(2024, 07, 29), schedule="0 * * * *") as dag:
#     etl_task = PythonOperator(
#         task_id='etl_task',
#         python_callable=main
#     )
