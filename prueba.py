import env
import datetime
import requests
import os
import json
import pandas as pd 
import time


# Dev Modules
from utils import RedShift, clean, Initialize, New_columns, Upload_Redshift

class Zendesk_support(RedShift): 
    def __init__(self, fecha = None, tipo = "complete", table = None): 
        """
        Extraccion de tickets desde Zendesk Support.
        """
        super().__init__()
        self.incremental = "https://runahr.zendesk.com/api/v2/incremental/tickets.json"
        self.tipo = tipo
        self.fecha = fecha
        
        if table == "tickets": 
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
            response = requests.get(self.incremental + str("?start_time=")+ str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            url = data['next_page']
        if self.tipo == "partial": 
            fecha = int(datetime.datetime.strptime(self.fecha, "%Y-%m-%d").timestamp())
            url = self.incremental + str("?start_time=")+ str(fecha)+ str("&include=comment_events")
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            url = data['next_page']
        while url: 
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code == 429: 
                print(url)
                print(response.json())
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


if __name__ == "__main__": 
    orgs = Zendesk_support(table = "comments")
    orgs.Comments()
