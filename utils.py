from sqlalchemy import create_engine
import os

class RedShift(): 
    def __init__(self, schema = "zendesk_support_pruebas"):
        self.engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:{port}/{base}".format(user = os.environ['REDSHIFT_USER'], 
                                                                                                        contr = os.environ['REDSHIFT_PASSWORD'], 
                                                                                                        base = os.environ['REDSHIFT_DATABASE'],
                                                                                                        host = os.environ['REDSHIFT_HOST'],
                                                                                                        port = os.environ['REDSHIFT_PORT']),
                                                                                                        connect_args={'sslmode': 'prefer',
                                                                                                                      'options': '-csearch_path={}'.format(schema)},
                                                                                                    echo = False, encoding = 'utf8')