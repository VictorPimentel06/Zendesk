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
    def __init__(self, fecha = None, tipo = "complete", table = None, ids = None): 

        super().__init__()
        self.tipo = tipo
        self.fecha = fecha
        self.search_url = "https://runahr.zendesk.com/api/v2/search.json?query="
        self.incremental_users = "https://runahr.zendesk.com/api/v2/incremental/users.json"
        self.ids = ids

        if table == "tickets": 
                try: 
                    self.__tickets_extract()
                except: 
                    print("Errores en la extraccion de tickets")
                    exit()
        if table == "users": 
            if self.tipo == "complete":
                try: 
                    self.__users_extract_complete()
                except: 
                    print("Errores en la extraccion de usuarios")
                    exit()
            if self.tipo == "partial": 
                self.__users_extract_partial()
        if table == "orgs": 
            if self.tipo == "complete":
                try: 
                    self.__orgs_extract()
                except: 
                    print("Errores en la extraccion de organizaciones")
                    exit()
            else: 
                self.__orgs_extract_partial()
            
        if table == "field_history": 
            if self.tipo == "complete": 
                try: 
                    self.__extract_tickets_audits()
                except: 
                    print("Errores en la extraccion de field_history")
                    exit()
            else: 
                self.__extract_tickets_audits_partial(ids= self.ids )

        if table == "field_options": 
            try: 
                self.__extract_field_options()
            except: 
                print("Errores en la extraccion de Field Options")
                exit()

        if table == "tag_history": 
            if self.tipo == "complete": 
                try: 
                    self.__extract_tag_history()
                except: 
                    print("Errores en la Extraccion de Tag History")
                    exit()
            else: 
                self.__extract_tag_history_partial(fecha = None, ids = self.ids)
        
        if table == "groups": 
            try: 
                self.extract_groups()
            except: 
                print("Errores en la extraccion de grupos")
                exit()
        
        if table == "group_members": 
            try: 
                self.extract_group_members()
            except: 
                print("Errores en la extraccion de miembros de grupos")
                exit()
        if table == "comments": 
            if self.tipo == "complete":
                try: 
                    self.__extract_comments()
                except: 
                    print("Errores en la extraccion de Comentarios")
            else: 
                self.__extract_comments_partial(ids = self.ids)

    def __tickets_extract(self): 
        """
        Extraccion completa de Tickets a traves del endpoint incremental. 

        Retoma los valores pasados como argumentos en la instancia. 
        Fecha: Valor en timestamp desde la cual se hara la extraccion. 
        tipo : 
            - complete: Extraccion de la totalidad de los tickets desde el primero de enero de 2018. 
            - partial: se tomara el valor de la fecha de entrada para hacer la extraccion. 
        """
        self.incremental = "https://runahr.zendesk.com/api/v2/incremental/tickets.json"
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
                                "removable": i["removable"] # if removable == False entonces es uncampo de sistema.  
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

        # Creacion de tabla catalogo de los custom fields siempre se actualizara por completo
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
            time.sleep(4)
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            tickets.extend(data['tickets'])
            if url == data['next_page']:
                break
            print("Numero de tickets extraidos: {}".format(len(tickets)))
            url = data["next_page"]
        tabla = pd.io.json.json_normalize(tickets)
        self.dic_fields = extract_custom_fields()
        tabla = add_field(tabla)
        tabla = clean.fix_columns(tabla)
        self.tickets_table = tabla

        print("Comienza Extraccion de Custom Fields")
        tabla = []
        for i in self.dic_fields.keys(): 
            aux = self.dic_fields[i]
            tabla.append(aux)
        tabla = pd.DataFrame(tabla)
        tabla["id"] = [i for i in self.dic_fields.keys()]
        tabla = clean.fix_columns(tabla)
        self.fields_table = tabla
        if self.tipo == "complete": 
            Initialize("custom_fields", self.engine)
            New_columns(tabla, "custom_fields", self.engine)
            Upload_Redshift(tabla,"custom_fields", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(tabla, "custom_fields", self.engine)
            Upload_Redshift(tabla,"custom_fields", "zendesk_support","zendesk-runahr",self.engine)
        print("Termino extraccion de custom fields")

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
            if int(index) % 1000 == 0: 
                print("tags del ticket: " + str(index))
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
    
    def __users_extract_complete(self): 
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
    
    def __users_extract_partial(self): 
        """
        No se puede usar el incremental export  porque todos los usuarios estan siendo 
        updated conforme a sus valores en Salesforce. 
        Para utilizar el enpoint incremental sera necesario modificar el flujo de trabajo de
        la actualizacion del user type para que solo actualice aquellos usuarios que 
        tengan algun cambio en sus valores. 
        """
        users =[] 
        # fecha = int(datetime.datetime.strptime(self.fecha, "%Y-%m-%d").timestamp())
        # url = self.incremental_users + str("?start_time=")+ str(fecha)
        # response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        query = "type:user created>={}".format(self.fecha)
        response = requests.get(self.search_url + query, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = response.json()
        users.extend(data["results"])
        url = data["next_page"]
        print(url)
        while url:
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            users.extend(data['results'])
            if url == data['next_page']:
                break
            print("Numero de usuarios extraidos: {}".format(len(users)))
            url = data["next_page"]
            print(url)
        tabla = pd.io.json.json_normalize(users)
        tabla = clean.fix_columns(tabla)
        self.users_table = tabla
        return self
    
    def Users(self): 
        tabla = self.users_table
        column_list =  clean.column_list(self.users_table)
        tabla = tabla.drop([str(i) for i in column_list], axis = 1)
        if len(tabla) != 0: 
            if self.tipo == "complete": 
                # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
                # para insertarlas. 
                Initialize("zendesk_users", self.engine)
                New_columns(tabla, "zendesk_users", self.engine)
                Upload_Redshift(tabla,"zendesk_users", "zendesk_support","zendesk-runahr",self.engine)
            if self.tipo == "partial": 
                New_columns(tabla, "zendesk_users", self.engine)
                Upload_Redshift(tabla,"zendesk_users", "zendesk_support","zendesk-runahr",self.engine)
        else: 
            print("No se econtraron registros")

    def Users_tags(self): 
        tabla = self.users_table
        print(tabla["tags"])
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
            self.users_tags = final_table
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
            print("Numero de organizaciones extraidas: {}".format(len(orgs)))
            url = data["next_page"]
        tabla = pd.io.json.json_normalize(orgs)
        tabla = clean.fix_columns(tabla)
        self.orgs_table = tabla

    def __orgs_extract_partial(self): 
        query = "type:organization created>={}".format(self.fecha)
        orgs = []
        response = requests.get(self.search_url + query, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = response.json()
        orgs.extend(data["results"])
        url = data["next_page"]
        while url:
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            if response.status_code != 200: 
                print("Error en la extraccion. CodeError: "+ str(response.status_code))
            data = response.json()
            orgs.extend(data['results'])
            if url == data['next_page']:
                break
            print("Numero de organizaciones extraidas: {}".format(len(orgs)))
            url = data["next_page"]
        tabla = pd.io.json.json_normalize(orgs)
        tabla = clean.fix_columns(tabla)
        self.orgs_table = tabla
        return self
    
    def Orgs(self): 
        if len(self.orgs_table) == 0: 
            print("No se encontraron registros nuevos de organizaciones")
        else: 
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
        if len(self.orgs_table) == 0: 
            print("No se encontraron registros nuevos de organizaciones")
        else: 
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
                self.orgs_tags = final_table
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
        if len(self.orgs_table) == 0: 
            print("No se encontraron registros nuevos de organizaciones")
        else: 
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
            print("Total de Audits extraidos: " + str(len(audits)))
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

    def __extract_tickets_audits_partial(self, ids): 
        """
        Esta funcion tiene que trabajarse de nuevo, no existe forma de filtrar los audits que han 
        sido creados el dia de hoy, ni por query.
        En su lugar se hara la extraccion completa de los registros. 

        Se ha optado por utilizar los tickets creados por dia. Sin embargo, esto no es 
        escalabre porque tiene que hacer tantas llamadas como tickets hayan sido creados.  
        """
        dic = []
        for ticket in ids: 
            url = "https://runahr.zendesk.com/api/v2/tickets/{}/audits.json".format(str(ticket))
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            data = response.json()
            for audit in data["audits"]: 
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

    def __extract_field_options(self): 
        self.custom_fields_url = "https://runahr.zendesk.com/api/v2/ticket_fields.json"
        respuesta = requests.get(self.custom_fields_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = respuesta.json()
        fields = data["ticket_fields"]
        tabla = []
        for field in fields: 
            if "custom_field_options" in field.keys():
                data = field["custom_field_options"]
                for entry in data: 
                    tabla.append(entry)
        tabla = pd.DataFrame(tabla)
        tabla = clean.fix_columns(tabla)
        self.table_fields_options = tabla

    def field_options(self): 
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("field_option", self.engine)
            New_columns(self.table_fields_options, "field_option", self.engine)
            Upload_Redshift(self.table_fields_options,"field_option", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(self.table_fields_options, "field_option", self.engine)
            Upload_Redshift(self.table_fields_options,"field_option", "zendesk_support","zendesk-runahr",self.engine)

    def __extract_tag_history(self): 
        self.tag_history_url = "https://runahr.zendesk.com/api/v2/ticket_audits.json"
        response = requests.get(self.tag_history_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = response.json()
        next_url = data["before_url"]
        tags = []
        tags.append(data["audits"])
        counter = 0
        while next_url != None and counter < 1: 
            response = requests.get(next_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            data = response.json()
            tags.append(data["audits"])
            next_url = data["before_url"]
            print(len(tags)) #deprecate
            counter += 1
            tabla = pd.DataFrame()
        for request in tags: 
            for audit in request: 
                id = audit["ticket_id"]
                created_at = audit["created_at"]
                for evento in audit["events"]: 
                    if evento["type"] == "Change" and evento["field_name"] == "tags": 
                        aux = pd.DataFrame(evento["value"])
                        aux["id"] = id
                        aux["updated"] = created_at
                        tabla =  tabla.append(aux)
        tabla.rename(columns = {0: "tag"}, inplace = True)
        tabla = tabla.reset_index(drop = True)
        tabla = clean.fix_columns(tabla)
        self.tabla_tag_history = tabla

    def __extract_tag_history_partial(self, fecha, ids): 
        tabla = pd.DataFrame()
        for ticket in ids: 
            url = "https://runahr.zendesk.com/api/v2/tickets/{}/audits.json".format(str(ticket))
            response = requests.get(url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
            data = response.json()

            for audit in data["audits"]: 
                id = audit["ticket_id"]
                created_at = audit["created_at"]
                for evento in audit["events"]: 
                    if evento["type"] == "Change" and evento["field_name"] == "tags": 
                        aux = pd.DataFrame(evento["value"])
                        aux["id"] = id
                        aux["updated"] = created_at
                        tabla =  tabla.append(aux)
            print("Audists del ticket: ", str(ticket))
        tabla.rename(columns = {0: "tag"}, inplace = True)
        tabla = tabla.reset_index(drop = True)
        tabla = clean.fix_columns(tabla)
        self.tabla_tag_history = tabla  
    
    def tag_history(self): 
        if self.tipo == "complete": 
            # Borra la tabla anterior e inicializa una nueva con solo un ID, posteriormente comprueba las nuevas columnas 
            # para insertarlas. 
            Initialize("tag_history", self.engine)
            New_columns(self.tabla_tag_history, "tag_history", self.engine)
            Upload_Redshift(self.tabla_tag_history,"tag_history", "zendesk_support","zendesk-runahr",self.engine)
        if self.tipo == "partial": 
            New_columns(self.tabla_tag_history, "tag_history", self.engine)
            Upload_Redshift(self.tabla_tag_history,"tag_history", "zendesk_support","zendesk-runahr",self.engine)

    def extract_groups(self): 
        self.groups_url = "https://runahr.zendesk.com/api/v2/groups.json"
        response = requests.get(self.groups_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        tabla = []
        for i in response.json()["groups"]:
            tabla.append(i)
        tabla = clean.fix_columns(pd.DataFrame(tabla))
        self.table_groups = tabla
        Initialize("groups", self.engine)
        New_columns(self.table_groups, "groups", self.engine)
        Upload_Redshift(self.table_groups,"groups", "zendesk_support","zendesk-runahr",self.engine)
        return self
    
    def extract_group_members(self): 
        self.groups_members_url = "https://runahr.zendesk.com/api/v2/group_memberships.json"
        response = requests.get(self.groups_members_url, auth = (os.environ["ZENDESK_USER"], os.environ["ZENDESK_PASSWORD"]))
        data = response.json()
        tabla= []
        for user in data["group_memberships"]: 
            tabla.append(user)
        tabla = clean.fix_columns(pd.DataFrame(tabla))
        self.table_groups_members = tabla
        Initialize("groups_members", self.engine)
        New_columns(self.table_groups_members, "groups_members", self.engine)
        Upload_Redshift(self.table_groups_members,"groups_members", "zendesk_support","zendesk-runahr",self.engine)
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

    def __extract_comments_partial(self, ids): 
        array = [i for i in ids]
        final_table = pd.DataFrame()
        for ticket in array: 
            respuesta = requests.get("https://runahr.zendesk.com/api/v2/tickets/{}/comments.json".format(str(ticket)), 
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
            print("Comentarios del ticket:", str(ticket))
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

if __name__ == "__main__": 
    instancia = Zendesk_support(table = "tickets", tipo = "complete", fecha = "2020-06-01")
    instancia.Tickets()
    instancia.Tickets_tags()