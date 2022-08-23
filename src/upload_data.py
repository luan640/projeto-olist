import os
import pandas as pd
import sqlalchemy

user = 'root'
psw = '15512332'
host = '127.0.0.1'
port = '3306'

#str_connection = 'sqlite:///{path}'
str_connection = 'mysql+pymysql://{user}:{psw}@{host}:{port}'

#os enderecos de nossos projetos e sub pastas
BASE_DIR = os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) )
DATA_DIR = os.path.join( BASE_DIR, 'data' )

'''#forma 1
files_names = os.listdir( DATA_DIR )
correct_files = []
for i in files_names:
    if i.endswith(".csv"):
        correct_files.append(i)
'''

#forma 2 compression list(criando lista de forma mais limpa, so entra no for se o if for verdadeiro)
files_names = [ i for i in os.listdir( DATA_DIR ) if i.endswith('.csv') ]

#abrindo conexao com banco
str_connection = str_connection.format( user=user, psw=psw, host=host, port=port ) 
connection = sqlalchemy.create_engine( str_connection ) 

#para cada arquivo é realizado uma inserção no banco
for i in files_names:
    df_tmp = pd.read_csv( os.path.join( DATA_DIR, i ) )
    table_name = "tb_" + i.strip('.csv').replace("olist_", "").replace("_dataset", "")
    df_tmp.to_sql( table_name,
                   connection,
                   schema='olist',
                   if_exists='replace',
                   index=False )


