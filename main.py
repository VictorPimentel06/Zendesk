
# Modulos Basicos
import env
import datetime
import requests
import os
import json
import pandas as pd 


# Dev Modules
from utils import RedShift, clean, Initialize, New_columns, Upload_Redshift

class Zendesk_support(RedShift): 
    def __init__(self, fecha = None, tipo = "complete"): 
        """
        Extraccion de tickets desde Zendesk Support.
        """
        super().__init__()
        self.incremental = "https://runahr.zendesk.com/api/v2/incremental/tickets.json?start_time="
        self.tipo = tipo
        self.fecha = fecha
        
        try: 
            self.__tickets_extract()
        except: 
            print("Errores en la extraccion de tickets")
            exit()

    def __tickets_extract(self): 
        """
        Extraccion completa de Tickets a traves del endpoint incremental. 
        
        Retoma los valores pasados como argumentos en la instancia. 
        Fecha: Valor en timestamp desde la cual se hara la extraccion. 
        tipo : 
            - complete: Extraccion de la totalidad de los tickets desde el primero de enero de 2018. 
            - partial: se tomara el valor de la fecha de entrada para hacer la extraccion. 
        """
        tickets = []
        if self.tipo == "complete": 
            fecha = int(datetime.datetime.strptime("2018-01-01","%Y-%m-%d").timestamp())
            response = requests.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            url = data['next_page']
        if self.tipo == "partial": 
            fecha = int(datetime.datetime.strptime(self.fecha, "%Y-%m-%d").timestamp())
            response = requests.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            url = data['next_page']
        while url: 
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            if url == data['next_page']:
                break
            print("Numero de tickets extraidos: {}".format(len(tickets)))
            url = data["next_page"]
        tabla = pd.io.json.json_normalize(tickets)
        tabla = clean.fix_columns(tabla)
        self.tickets_table = tabla

    def Tickets(self):
        column_list =  clean.column_list(self.tickets_table)
        tabla = tabla.drop([str(i) for i in column_list], axis = 1)
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("tickets", self.engine)
            New_columns(tabla, "tickets", self.engine)
            Upload_Redshift(tabla,"tickets", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(tabla, "tickets", self.engine)
            Upload_Redshift(tabla,"tickets", "zendesk_support","zendesk-runahr",self.engine)
    
    def Tickets_tags(self): 
        final_table = pd.DataFrame()
        column_list =  clean.column_list(self.tickets_table)
        tabla = self.tickets_table[column_list + ["id"]]
        tabla = pd.concat([pd.DataFrame({"tags":tabla['tags']}),pd.DataFrame({"id":tabla['id']})], axis = 1)
        for index, row in tabla.iterrows(): 
            id = row['id']
            tags = row['tags']
            inter_table = pd.DataFrame(tags, columns= ['tags'])
            inter_table['id'] = [id for i in range(len(inter_table)) ]
            final_table = final_table.append(inter_table)
        final_table.reset_index(inplace= True,drop= True )
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("tickets_tags", self.engine)
            New_columns(final_table, "tickets_tags", self.engine)
            Upload_Redshift(final_table,"tickets_tags", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(final_table, "tickets_tags", self.engine)
            Upload_Redshift(final_table,"tickets_tags", "zendesk_support","zendesk-runahr",self.engine)
        
        

if __name__ == "__main__": 
    instance = Zendesk_support(fecha = "2020-05-01",tipo = "complete")
    instance.Tickets_tags()