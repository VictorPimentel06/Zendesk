
# Modulos Basicos
import env
import datetime
import requests
import os
import json
import pandas as pd 
import time
from pandas.io.json import json_normalize


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
        if table == "users": 
            try: 
                self.__users_extract()
            except: 
                print("Errores en la extraccion de usuarios")
                exit()
        if table == "organizations": 
            try: 
                self.__orgs_extract()
            except: 
                print("Errores en la extraccion de organizaciones")
                exit()
        if table == "comments": 
            try: 
                self.__extract_comments()
            except: 
                print("Errores en la extraccion de comentarios")
                exit()
        if table == "fields": 
            try: 
                self.__extract_custom_fields()
            except: 
                print("Errores en la extraccion")
                exit()
        if table == "ticket_history": 
            try: 
                self.__extract_tickets_audits()
            except: 
                print("Errores en la extracion")
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
        def extract_custom_fields(): 
            self.custom_fields_url = "https://runahr.zendesk.com/api/v2/ticket_fields.json"
            respuesta = requests.get(self.custom_fields_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            data = respuesta.json()
            fields = data["ticket_fields"]
            dic = {}
            for i in fields: 
                dic.update({i["id"]:
                                {
                                "Name":i["raw_title_in_portal"], 
                                "Description":i["description"],
                                "Raw Description": i["raw_description"],
                                "Created_at": i["created_at"], 
                                "removable": i["removable"] # ir removable == False entonces es uncampo de sistema.  
                                }
                        })
            return  dic
        def add_field(tickets_table):
            dic = {}
            for ticket, fields in zip(tickets_table.id, tickets_table.custom_fields): 
                for field in fields: 
                    if ticket not in dic: 
                        dic.update(
                            {ticket: 
                                {
                                    field["id"]:
                                        {
                                        "value": field["value"],
                                        "name":self.dic_fields[field["id"]]["Name"]
                                        }
                                }
                                })
                    else: 
                        dic[ticket].update(
                                {
                                    field["id"]:
                                        {
                                        "value": field["value"],
                                        "name":self.dic_fields[field["id"]]["Name"]
                                        }
                                }
                        )

            tabla = pd.DataFrame.from_dict(dic).T
            for column in tabla.columns: 
                nombre = "Custom_" + str(tabla[column].iloc[0]["name"])
                tabla = tabla.rename(columns = {column: nombre})
                aux = []
                for record in tabla[nombre]: 
                    aux.append(record["value"])
                tabla[nombre] = aux
            tabla.reset_index(inplace= True)
            tabla = tabla.rename(columns = {"index": "ticket_id"})
            tabla = tabla.merge(tickets_table, left_on = "ticket_id", right_on= "id")
            return tabla

        #Extraccion de campos adicionales
        self.dic_fields = extract_custom_fields()

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
            url = self.incremental + str("?start_time=")+ str(fecha)
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
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
        tabla = add_field(tabla)
        tabla = clean.fix_columns(tabla)
        self.tickets_table = tabla


    def Tickets(self):
        tabla = self.tickets_table
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
    def __users_extract(self): 
        users = []
        self.users_url = "https://runahr.zendesk.com/api/v2/users.json"
        response = requests.get(self.users_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = response.json()
        users.extend(data['users'])
        url = data['next_page']
        while url:
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            users.extend(data['users'])
            if url == data['next_page']:
                break
            print("Numero de usuarios extraidos: {}".format(len(users)))
            url = data["next_page"]
        tabla = pd.io.json.json_normalize(users)
        tabla = clean.fix_columns(tabla)
        self.users_table = tabla
    def Users(self): 
        tabla = self.users_table
        column_list =  clean.column_list(self.users_table)
        tabla = tabla.drop([str(i) for i in column_list], axis = 1)
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("zendesk_users", self.engine)
            New_columns(tabla, "zendesk_users", self.engine)
            Upload_Redshift(tabla,"zendesk_users", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(tabla, "zendesk_users", self.engine)
            Upload_Redshift(tabla,"zendesk_users", "zendesk_support","zendesk-runahr",self.engine)
    def Users_tags(self): 
        tabla = self.users_table
        column_list =  clean.column_list(self.users_table)
        tabla = tabla[column_list + ["id"]]
        tabla = pd.concat([pd.DataFrame({"tags":tabla['tags']}),pd.DataFrame({"id":tabla['id']})], axis = 1)
        final_table = pd.DataFrame()
        for index, row in tabla.iterrows(): 
            if len(row['tags']) == 0:
                pass
            else: 
                id = row['id']
                tags = row['tags']
                inter_table = pd.DataFrame(tags, columns= ['tags'])
                inter_table['id'] = [id for i in range(len(inter_table)) ]
                final_table = final_table.append(inter_table)
        try: 
            final_table.reset_index(inplace= True,drop= True )
        except: 
            pass

        if len(final_table) == 0: 
            self.users_tags == final_table
        else: 
            if self.tipo == "complete": 
                # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
                # para insertarlas. 
                Initialize("user_tags", self.engine)
                New_columns(final_table, "user_tags", self.engine)
                Upload_Redshift(final_table,"user_tags", "zendesk_support","zendesk-runahr",self.engine)
            if self.tipo == "partial": 
                New_columns(final_table, "user_tags", self.engine)
                Upload_Redshift(final_table,"user_tags", "zendesk_support","zendesk-runahr",self.engine)
            self.user_tags =final_table
    def __orgs_extract(self): 
        """
        Se incluye la opcion de count
        """
        self.orgs_url = "https://runahr.zendesk.com/api/v2/organizations.json"
        response = requests.get(self.orgs_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        count = response.json()['count']
        data = response.json()
        orgs = []
        orgs.extend(data['organizations'])
        url = data['next_page']
        while url:
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            orgs.extend(data['organizations'])
            if url == data['next_page']:
                break
            print("Numero de usuarios extraidos: {}".format(len(orgs)))
            url = data["next_page"]
        tabla = pd.io.json.json_normalize(orgs)
        tabla = clean.fix_columns(tabla)
        self.orgs_table = tabla
    def Orgs(self): 
        tabla = self.orgs_table
        column_list =  clean.column_list(self.orgs_table)
        tabla = tabla.drop([str(i) for i in column_list], axis = 1)
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("orgs", self.engine)
            New_columns(tabla, "orgs", self.engine)
            Upload_Redshift(tabla,"orgs", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(tabla, "orgs", self.engine)
            Upload_Redshift(tabla,"orgs", "zendesk_support","zendesk-runahr",self.engine)
    def Orgs_tags(self): 
        tabla = self.orgs_table
        column_list =  clean.column_list(self.orgs_table)
        tabla = tabla[column_list + ["id"]]
        tabla = pd.concat([pd.DataFrame({"tags":tabla['tags']}),pd.DataFrame({"id":tabla['id']})], axis = 1)
        final_table = pd.DataFrame()
        for index, row in tabla.iterrows(): 
            if len(row['tags']) == 0:
                pass
            else: 
                id = row['id']
                tags = row['tags']
                inter_table = pd.DataFrame(tags, columns= ['tags'])
                inter_table['id'] = [id for i in range(len(inter_table)) ]
                final_table = final_table.append(inter_table)
        try: 
            final_table.reset_index(inplace= True,drop= True )
        except: 
            pass
        if len(final_table) == 0: 
            self.orgs_table = final_table
        else: 
            if self.tipo == "complete": 
                # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
                # para insertarlas. 
                Initialize("orgs_tags", self.engine)
                New_columns(final_table, "orgs_tags", self.engine)
                Upload_Redshift(final_table,"orgs_tags", "zendesk_support","zendesk-runahr",self.engine)
            if self.tipo == "partial": 
                New_columns(final_table, "orgs_tags", self.engine)
                Upload_Redshift(final_table,"orgs_tags", "zendesk_support","zendesk-runahr",self.engine)
            self.orgs_tags =final_table
    def Orgs_domains(self): 
        tabla = self.orgs_table 
        tabla = tabla[["domain_names","id"]]
        final_table = pd.DataFrame()
        for index, row in tabla.iterrows(): 
            if len(row['domain_names']) == 0:
                pass
            else: 
                id = row['id']
                tags = row['domain_names']
                inter_table = pd.DataFrame(tags, columns= ['domain_names'])
                inter_table['id'] = [id for i in range(len(inter_table)) ]
                final_table = final_table.append(inter_table)
        try: 
            final_table.reset_index(inplace= True,drop= True )
        except: 
            pass
        if len(final_table) == 0: 
            self.orgs_table = final_table
        else: 
            if self.tipo == "complete": 
                # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
                # para insertarlas. 
                Initialize("orgs_domains", self.engine)
                New_columns(final_table, "orgs_domains", self.engine)
                Upload_Redshift(final_table,"orgs_domains", "zendesk_support","zendesk-runahr",self.engine)
            if self.tipo == "partial": 
                New_columns(final_table, "orgs_domains", self.engine)
                Upload_Redshift(final_table,"orgs_domains", "zendesk_support","zendesk-runahr",self.engine)
            self.orgs_domains =final_table
        return self

    def __extract_comments(self): 
        """
        Se hace a traves de los ids de los tickets que ya estan creados en Zendesk. 
        Solo se insertaran los datos de body, created_at, id, y si el comentario es publico o no. 
        """
        response = self.engine.execute("SELECT id from zendesk.tickets")
        array = [i[0] for i in response]
        final_table = pd.DataFrame()
        for ticket in array: 
            respuesta = requests.get("https://runahr.zendesk.com/api/v2/tickets/{}/comments.json".format(ticket), 
                                auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if respuesta.status_code != 200: 
                print(respuesta.text)
                continue
            data = respuesta.json()
            comments = data["comments"]
            ticket_comments = pd.DataFrame(comments)
            ticket_comments = ticket_comments[["body", "created_at", "id", "public"]]
            ticket_comments["ticket_id"] = ticket
            final_table = final_table.append(ticket_comments)
            if len(final_table) % 1000 == 0: 
                print("Tickets extraidos hasta ahora: " + str(len(final_table)))
        final_table.reset_index(inplace= True,drop= True )
        self.Comments_table = final_table
    def Comments(self): 
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("ticket_comments", self.engine)
            New_columns(self.Comments_table, "ticket_comments", self.engine)
            Upload_Redshift(self.Comments_table,"ticket_comments", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(self.Comments_table, "ticket_comments", self.engine)
            Upload_Redshift(self.Comments_table,"ticket_comments", "zendesk_support","zendesk-runahr",self.engine)
        return self
    
    def __extract_tickets_audits(self): 
        self.audits_url = "https://runahr.zendesk.com/api/v2/ticket_audits.json"
        response = requests.get(self.audits_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = response.json()
        next_url = data["before_url"]
        audits = []
        audits.append(data["audits"])
        while next_url != None : 
            response = requests.get(next_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            data = response.json()
            audits.append(data["audits"])
            next_url = data["before_url"]
            print(len(audits))
        dic = []
        for request in audits: 
            for audit in request: 
                id = audit["ticket_id"]
                created_at = audit["created_at"]
                for evento in audit["events"]: 
                    if evento["type"] == "Change" and evento["field_name"] != "tags": 
                        dic.append([evento["field_name"], evento["value"], evento["previous_value"], id, created_at])
        tabla = pd.DataFrame(dic, columns = ["field_name", "value", "previous_value", "id", "updated_at"])
        self.tabla_field_history = tabla
    def field_history(self): 
        """
        La tabla de field_history no incluye a los tickets que esten archivados. 
        """
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("field_history", self.engine)
            New_columns(self.tabla_field_history, "field_history", self.engine)
            Upload_Redshift(self.tabla_field_history,"field_history", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(self.tabla_field_history, "field_history", self.engine)
            Upload_Redshift(self.tabla_field_history,"field_history", "zendesk_support","zendesk-runahr",self.engine)
        return self
        
if __name__ == "__main__": 
    tickets = Zendesk_support(fecha = "2020-05-24",tipo = "complete", table = "ticket_history")
    tickets.field_history()
    
    #     if ticket in dic: clear
    #         pass
    #     else: 
    #         for field in fields:

    #                 dic[ticket] = dic[ticket] + [tickets.dic_fields[field["id"]]]
    #             else: 
    #                 dic.update({ticket:[tickets.dic_fields[field["id"]]]}) 
    
    # for i in dic: 
    #     print(dic[i], i)
    #     print()
    