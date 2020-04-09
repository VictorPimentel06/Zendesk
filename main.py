import env
import datetime
import requests
import os
import json


class Zendesk_support(): 
    def __init__(self): 
        """
        Extraccion de tickets desde Zendesk Support.
        """
        self.incremental = "https://runahr.zendesk.com/api/v2/incremental/tickets.json?start_time="
        self.session = requests.Session()

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
            fecha = int(datetime.datetime.strptime("2018-01-01","%Y-%M-%d").timestamp())
            response = self.session.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"])).json()
            tickets.extend(response['tickets'])
        if tipo == "partial": 
            fecha = int(datetime.datetime.strptime(fecha, "%Y-%M-%d").timestamp())
            response = self.session.get(self.incremental + str(fecha), auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"])).json()

        while response['next_page']: 
            tickets.extend(response['tickets'])
            response = self.session.get(response['next_page'], auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"])).json()
            print(response['next_page'])
            
            





Zendesk_support().Tickets(fecha = "2020-04-01",tipo = "partial")