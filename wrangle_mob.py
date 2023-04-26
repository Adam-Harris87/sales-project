import numpy as np
import pandas as pd
import os

#----------------------------------------------

def acquire_mob_sales():
    '''
    This function will read in tables from 3 xlsm excel documents containing sales
    order history for customers
    '''
    # get sales order history for customers
    sales = retention_18 = pd.read_excel('Retention Grid.xlsm', sheet_name=18)
    sorder_details = raw_6 = pd.read_excel('Raw Forecast v2.xlsm', sheet_name=6)
    # get a customer list
    customer_list = retention_17 = pd.read_excel('Retention Grid.xlsm', sheet_name=17)
    # return the 4 dataframes
    return sales, sorder_details, customer_list

#----------------------------------------------

def acquire_mob_item_history():
    '''
    This function will retreive a table from an xlsm excel document containing
    item sales history by week for all products
    '''
    # get item sales history
    item_sales = pd.read_excel('Sales Forecast.xlsm', sheet_name=1)
    # return dataframe
    return item_sales

#----------------------------------------------

def prepare_mob_item_history(all_sales_history):
    '''
    This function will take in item sales history for all products, it will then
    drop unnecessary columns and change the change the index to datetime format,
    it will then change the sku names of products to generic numbers.
    '''
    # remove items that are inactive products
    sales_history = all_sales_history[all_sales_history.Forecast == True
                  # remove unnecessary columns
                 ].drop(columns=['Lifetime', 'Desc', 'Forecast'
                                 # remove extra columns that have no data
                                ]).dropna(axis=1
                                          # change the column headers to item_sku
                                         ).set_index('SKU').T
    # convert the index to datetime
    sales_history.index = pd.to_datetime(sales_history.index)
    # resample the index to standard week format
    sales_history = sales_history.resample('W').sum()
    # replace product skus with generic numbers for infosec
    sales_history.columns = ['prod_' + str(x) for x in range(0,len(sales_history.columns))]
    # return the prepared dataframe
    return sales_history

#----------------------------------------------

def prepare_mob_sales_order(sales, sorder_details, customer_list):
    '''
    This function will take in dataframes of sales orders, the order details, and a list
    of customers. it will then merge the dfs, fill in null values with appropriate values,
    remove suspended customers, and then remove some now unnecessary info.
    '''
    # merge sales orders with the sales order details
    sales_orders = pd.merge(left=sales, 
                            right=sorder_details, 
                            how='left', 
                            on='OrderID')
    # fill na values
    sales_orders.QtyOrdered.fillna(1, inplace=True)
    sales_orders.QtyShipped.fillna(0, inplace=True)
    # get a list of suspended customers from the customer list, since we don't want
    # suspended customers in the final report
    suspended_list = customer_list[['CustomerID', 'Suspended'
                               ]][customer_list.Suspended == False]
    # merge the suspended list with the sales orders
    sorders = pd.merge(left=sales_orders, 
                       right=suspended_list, 
                       how='left', 
                       on='CustomerID')
    # suspended customers will ahve a null value in the Suspended column,
    # drop the suspended info then remove the suspended column
    sorders = sorders[sorders.Suspended.isna() == False].drop(columns='Suspended')
    # change column names to lowercase
    sorders.columns = [col.lower() for col in sorders]
    # set the index to the orderdate so we can work with the data as a time series problem
    sorders = sorders.set_index('orderdate').sort_index()
    # remove the one row that has decimal qty ordered and qty_shipped
    sorders = sorders[~(sorders.qtyordered.astype(int) != sorders.qtyordered)]
    # convert the float type columns to int
    sorders.qtyordered = sorders.qtyordered.astype(int)
    sorders.qtyshipped = sorders.qtyshipped.astype(int)
    # change column names to be more readable
    cols = ['order_id', 'order_no', 'customer_id', 'order_status', 'order_amount',
         'seq', 'qty_ordered', 'qty_shipped', 'item_id']
    sorders.columns = cols
    # return the prepared sales orders dataframe
    return sorders

#----------------------------------------------

def wrangle_mob_item_sales():
    '''
    This function will check for existance of item sales history csv file in the local
    directory, if one does not exist it will acquire the dataset, prepare it, then 
    create a csv file and either way it will return the prepared dataframe.
    '''
    # check for existance of item_history.csv file in the local directory
    if os.path.exists('item_history.csv'):
        # read in csv file if one exists
        item_history = pd.read_csv('item_history.csv', index_col=0)
    # if csv file does not exist
    else:
        # read in dataset from excel file
        all_item_history = acquire_mob_item_history()
        # prepare the data
        item_history = prepare_mob_item_history(all_item_history)
        # write a new csv file to the local directory
        item_history.to_csv('item_history.csv')
    # return the prepared dataframe
    return item_history

#----------------------------------------------

def wrangle_mob_sales():
    '''
    This function will check for existance of sales orders history csv file in the local
    directory, if one does not exist it will acquire the dataset, prepare it, then 
    create a csv file and either way it will return the prepared dataframe.
    '''
    # check for existance of item_history.csv file in the local directory
    if os.path.exists('sales_history.csv'):
        # read in csv file if one exists
        sales = pd.read_csv('sales_history.csv', index_col=0)
    # if csv file does not exist
    else:
        # read in dataset from excel file
        sales_history, sorder_details, customer_list = acquire_mob_sales()
        # prepare the data
        sales = prepare_mob_sales_order(sales_history, sorder_details, customer_list)
        # write a new csv file to the local directory
        sales.to_csv('sales_history.csv')
    # return the prepared dataframe
    return sales