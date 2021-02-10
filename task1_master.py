import pandas as pd
import numpy as np

# read the excel file
file_path = 'Master.xlsx'
df = pd.read_excel(
    file_path, index_col=None, header=None)

# initialize the variable for do this task
tables = []
split_tables = []
update_dates = {}
start_index = {}
end_index = {}
current_table = None

# find start row, end row, and last update date of each table, and add result of each table in key and value
for index, row in df.iterrows():
    col_1, col_2, col_3, col_4 = row

    if str(col_1).startswith('Table Name: '):
        name_table = str(col_1).replace('Table Name:', '').strip()
        tables.append(name_table)
        current_table = name_table
    elif current_table is not None:
        if str(col_1).startswith('Last Update Date:'):
            last_update_date = str(col_1).replace(
                'Last Update Date:', '').strip()
            modified_date = last_update_date[0:4] + '-' + last_update_date[4:6] + '-' + last_update_date[6:]
            update_dates[name_table] = modified_date
            start_index[name_table] = index + 1
        else:
            if index == len(df) - 1:
                end_index[name_table] = index + 1
            else:
                end_index[name_table] = index

# this function do for split each table in dataframe and transform the table in appropriate format
def split_table(name_table):

    split_table = df[start_index[name_table] : end_index[name_table]]
    split_table = split_table.dropna(how='all', axis=1)
    header_row = split_table.iloc[0]
    split_table.columns = header_row
    split_table = split_table.drop([start_index[name_table]])
    split_table = split_table.reset_index(drop=True)
    split_table['last_updated_date'] = update_dates[name_table]
    
    return split_table

# this function do for add zero in the customer ID to become 7 digits
def add_customer_digit(split_df):

    digit_str = str(split_df['custId'])
    length_digit = len(digit_str)

    if length_digit < 7:
        return digit_str.zfill(7)
    
    return digit_str

# split each table in dataframe by calling the split table function
for table in tables:
    name_split_df = table.replace(' ', '_').lower()
    split_tables.append(name_split_df)
    exec('{} = split_table(table)'.format(name_split_df))

# check and add the zero digit to be 7 digit in customer ID by calling add customer digit function
customer_master['custId'] = customer_master.apply(add_customer_digit, axis=1)

# print name and preview of each split table
for table in split_tables:
    print('Name of split table: {}'.format(table))
    print('Preview of split table')
    exec('print({}.head(3))'.format(table))