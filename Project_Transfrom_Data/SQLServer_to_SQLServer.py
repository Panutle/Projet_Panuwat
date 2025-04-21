import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

#Connect to Database
def Connect():
    SERVER = 'localhost'
    DATABASE = 'Test_db_2'
    USERNAME = 'sa'
    PASSWORD = 'YourStrong@Passw0rd'

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD)
    return conn

#Select Name Columns
def Sel_Name_Col(conn, N_Table):
    SQL_COLUMNS = f'''
    select COLUMN_NAME
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_NAME='{N_Table}'
    '''

    col = conn.cursor().execute(SQL_COLUMNS).fetchall()

    col_ = [i[0] for i in col]

    return col_

#Select Data
def Sel_Data(conn, N_Table):
    SQL_QUERY = f"""
    SELECT *
    FROM [dbo].[{N_Table}]
    """

    data = conn.cursor().execute(SQL_QUERY).fetchall()

    return data

#Create DF
def DF(data, col_):
    aa = {}
    for i in range(len(col_)):
        aa[col_[i]] = [j[i] for j in data]
    df = pd.DataFrame(aa)
    
    return df

#Create DF
def DF_2(df):
    df_2 = df.drop([0])
    df_2[' '] = df_2[' '].replace('Delete','Free')

    return df_2

#Export to sql
def Exp_to_sql(df, Name):
    SERVER = 'localhost'
    DATABASE = 'Test_db_2'
    USERNAME = 'sa'
    PASSWORD = 'YourStrong@Passw0rd'

    conn = quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD)

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn}')

    df.to_sql(f'{Name}', schema= 'dbo', con= engine, index=False, if_exists='replace')

    print('Success')

#Upsert Data
def Upsert_data(conn, N_Table_Target, N_Table_Source, Col_Condition, col_):
    SQL_QUERY_3 = f'''
    MERGE INTO [dbo].[{N_Table_Target}] AS Target
    USING [dbo].[{N_Table_Source}]	AS Source
    ON Source.{Col_Condition} = Target.{Col_Condition}
    WHEN MATCHED THEN 
        UPDATE SET
        {','.join([f'Target.{i} = Source.{i}' for i in col_])}
    WHEN NOT MATCHED THEN
        INSERT (
            {','.join(col_)}
            ) 
        VALUES (
        {','.join([f'Source.{i}' for i in col_])}
        );
    '''

    conn.cursor().execute(SQL_QUERY_3)

    conn.commit()

#Main Func
def main():
    N1 = 'data_test_Q'

    N2 = 'data_test_Q_2'

    NT = ' '

    col = 'pID'

    conn = Connect()

    col_ = Sel_Name_Col(conn, NT)

    data = Sel_Data(conn, NT)

    df = DF(data, col_)

    df_2 = DF_2(df)

    Exp_to_sql(df, N1)

    Exp_to_sql(df_2,N2)

    Upsert_data(conn, N2, N1, col, col_)

if __name__ == "__main__":
    main()