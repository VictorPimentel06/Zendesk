
# Modulos Basicos
import env
import datetime
import requests
import os
import json


# Dev Modules
from utils.redshift_utils import RedShift

class Zendesk_support(): 
    def __init__(self): 
        """
        Extraccion de tickets desde Zendesk Support.
        """
        self.incremental = "https://runahr.zendesk.com/api/v2/incremental/tickets.json?start_time="

    def Tickets(self, fecha = None, tipo = "complete"): 
        """
        Extraccion completa de Tickets a traves del endpoint incremental. 
        
        Fecha: Valor en timestamp desde la cual se hara la extraccion. 
        tipo : 
            - complete: Extraccion de la totalidad de los tickets desde el primero de enero de 2018. 
            - extraccion: se tomara el valor de la fecha de entrada para hacer la extraccion. 
        """
        tickets = []
        if tipo == "complete": 
            fecha = int(datetime.datetime.strftime("2018-01-01","%Y-%m-%d").timestamp())
            response = requests.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            data = response.json()
            tickets.extend(response['tickets'])
        if tipo == "partial": 
            fecha = int(datetime.datetime.strptime(fecha, "%Y-%m-%d").timestamp())
            response = requests.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            url = data['next_page']
            tickets.extend(data['tickets'])
        while url: 
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            if url == data['next_page']:
                break
            url = data["next_page"]
            
motor = RedShift(schema= "salesforce").engine
response = motor.execute("SELECT count(*) from lead;")         
for i in response: 
    print(i)
            
            




# Zendesk_support().Tickets(fecha = "2020-01-01",tipo = "partial")