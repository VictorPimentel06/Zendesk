
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
    def __init__(self): 
        """
        Extraccion de tickets desde Zendesk Support.
        """
        super().__init__()
        self.incremental = "https://runahr.zendesk.com/api/v2/incremental/tickets.json?start_time="

    def Tickets(self, fecha = None, tipo = "complete"): 
        """
        Extraccion completa de Tickets a traves del endpoint incremental. 
        
        Fecha: Valor en timestamp desde la cual se hara la extraccion. 
        tipo : 
            - complete: Extraccion de la totalidad de los tickets desde el primero de enero de 2018. 
            - partial: se tomara el valor de la fecha de entrada para hacer la extraccion. 
        """
        tickets = []
        if tipo == "complete": 
            fecha = int(datetime.datetime.strptime("2018-01-01","%Y-%m-%d").timestamp())
            response = requests.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            url = data['next_page']
        if tipo == "partial": 
            fecha = int(datetime.datetime.strptime(fecha, "%Y-%m-%d").timestamp())
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
        # Generar lista de columnas para generar otra tabla
        column_list =  clean.column_list(tabla)
        tabla = tabla.drop([str(i) for i in column_list], axis = 1)
        if tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("tickets", self.engine)
            New_columns(tabla, "tickets", self.engine)
            Upload_Redshift(tabla,"tickets", "zendesk_support","zendesk-runahr",self.engine)
        if tipo == "partial": 
            New_columns(tabla, "tickets", self.engine)
            Upload_Redshift(tabla,"tickets", "zendesk_support","zendesk-runahr",self.engine)
        
        
        

if __name__ == "__main__": 
    Zendesk_support().Tickets(fecha = "2020-04-25",tipo = "complete")