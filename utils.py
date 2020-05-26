from sqlalchemy import create_engine
from io import StringIO
import os
import boto3

class RedShift(): 
    def __init__(self, schema = "zendesk"):
        self.engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:{port}/{base}".format(user = os.environ['REDSHIFT_USER'], 
                                                                                                        contr = os.environ['REDSHIFT_PASSWORD'], 
                                                                                                        base = os.environ['REDSHIFT_DATABASE'],
                                                                                                        host = os.environ['REDSHIFT_HOST'],
                                                                                                        port = os.environ['REDSHIFT_PORT']),
                                                                                                        connect_args = {'sslmode': 'prefer','options': '-csearch_path={}'.format(schema)},
                                                                                                        echo = False, encoding = 'utf8')

class Upload_Redshift() :
    def __init__(self, Dataframe, name, carpeta, bucket, engine) :
        """
        Clase que recibe como argumento un dataframe object de pandas

        - Dataframe: Dataframe de pandas 
        - Folder: carpeta dentro de bucket
        - Name: nombre del documento. Este debe tener extension .csv 
            ejemplo: 
                "prueba.csv"
        - bucket: bucket de AWS
        """
        s3 = boto3.client("s3",
            aws_access_key_id = os.environ["AWS_KEY"],
            aws_secret_access_key = os.environ["AWS_SECRET_KEY"])
        csv_buffer = StringIO()
        Dataframe.to_csv(csv_buffer, index = False)
        folder = str(carpeta) + "/" +str(name)+'.csv'
        s3.put_object(Bucket = bucket, Key = folder, Body = csv_buffer.getvalue())
    
        cols = Dataframe.columns
        with engine.connect() as conn :
            conn.execute("COPY {schema}.{table} ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1 EMPTYASNULL;commit;".format(schema = os.environ['REDSHIFT_SCHEMA'], 
                                                                                                                                                                                                    table = name,
                                                                                                                                                                                                    cols = ', '.join(cols[j] for j in range( len(cols) ) ),
                                                                                                                                                                                                    s3='s3://{}/{}/{}'.format(os.environ['AWS_BUCKET'],
                                                                                                                                                                                                                              carpeta,
                                                                                                                                                                                                                              name+".csv"),
                                                                                                                                                                                                    keyid = os.environ['AWS_KEY'],
                                                                                                                                                                                                    secretid= os.environ['AWS_SECRET_KEY']))
        
    

class clean(): 
    @staticmethod
    def fix_columns(datos) :
        NOT_SUPPORT_COLUMN_NAMES = ["from", "to", "user", "group"]
        columns = datos.columns
        for col in columns :
            if col in NOT_SUPPORT_COLUMN_NAMES: 
                col_new = str(col) + "_"
                col_new = col_new.replace(".","_")
            else: 
                col_new = col.replace(".", "_")
                col_new = col_new.replace(" ", "_")
            datos = datos.rename(columns = {col:col_new})
        return datos
    @staticmethod
    def column_list(datos): 
        """
        Recibe como argumento solo dataframes completos y regresa una lista 
        con las columnas que tienen valores de diccionario.
        """
        lista = []
        for column in datos.columns:
            if type(datos[column].iloc[0]) == list: 
                lista.append(str(column))
            else: 
                pass
        return lista

class Initialize() :
    def __init__(self, name, engine) :
        with engine.connect() as conn :
            conn.execute("DROP TABLE IF EXISTS {} CASCADE".format(name))
            conn.execute("CREATE TABLE {}(id character varying (1024) PRIMARY KEY)".format(name))

class New_columns(): 
    def __init__(self, tabla, name, engine):
        """ 
        Tabla: Dataframe a insertar
        name : nombre de la tabla en Redshift
        """ 
        types = {
            "int" : "bigint", 
            "int64": "bigint", 
            "bool": "boolean", 
            "float64": "double precision",
            "object": "character varying (65535)"
        }
        cols = tabla.columns
        try: 
            q = engine.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(name))
            self.cols_db = [i[0] for i in q]
            if len(self.cols_db) == 0: 
                self.cols = self.cols_db
            else: 
                self.cols = list(set(cols)-set(self.cols_db))
        except: 
            self.cols = []
        if len(self.cols) != 0: 
            print("Columnas Nuevas: " + str(self.cols))
            for j in self.cols: 
                engine.execute("ALTER TABLE {} ADD COLUMN {} {}".format(name, str(j),types[str(tabla[j].dtypes)]))