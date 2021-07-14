tabelle = ['pred_Isarco_Q_mean', 'pred_Isarco_W_mean', 'pred_Isarco_WT_mean', 'pred_Adige_Q_mean', 'pred_Adige_W_mean', 'pred_Adige_WT_mean', 'pred_Talvera_Q_mean', 'pred_Talvera_W_mean', 'pred_Talvera_WT_mean']
for nome_tabella in tabelle:
        create_table_query = '''
                CREATE TABLE {table_name}
                (
                Timestamp DATETIME NOT NULL,
                {var}_1h FLOAT (20,2) ,
                {var}_3h FLOAT (20,2) ,
                {var}_12h  FLOAT (20,2) ,
                {var}_1d  FLOAT (20,2) ,
                {var}_3d  FLOAT (20,2) ,
                {var}_1w  FLOAT (20,2) ,
                Id INT
                )
                '''.format(table_name = nome_tabella, var = nome_tabella[-6:])
        cursor.execute(create_table_query)