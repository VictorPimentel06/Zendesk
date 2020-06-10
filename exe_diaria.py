# La extraccion  diaria tendra que ser sobre los ids extraidos de los tickets
# debido a que la mayoria de los endpoints no permiten filtrado por fecha
# Hay que hacer modificacionesl main


from main import Zendesk_support
import threading
from datetime import date, datetime
import time




fecha = str(date.today())
# fecha = "2020-06-01"

"""
El valor del search tiene que ser mayor o igual en le query de extraccion

"""


def Users(fecha): 
    print("Comenzo Extraccion de Usuarios")
    instancia = Zendesk_support(table = "users", tipo= "partial", fecha = fecha)
    instancia.Users()
    print("Termino Extraccion de Usuarios")
    print()

    print("Comienza Extraccion de User Tags")
    instancia.Users_tags()
    print("Termina Extraccion de User Tags")
    print()

def Tickets(fecha): 
    print("Comenzo Extraccion de Tickets")
    instancia = Zendesk_support(table = "tickets", tipo = "partial", fecha = fecha)
    instancia.Tickets()
    instancia.Tickets_tags()
    print("Termino Extraccion de Tickets")
    print()

def Organizations(fecha): 
    print("Comenzo Extraccion de Organizaciones")
    instancia = Zendesk_support(table = "orgs", tipo = "partial", fecha = fecha)
    instancia.Orgs()
    instancia.Orgs_tags()
    instancia.Orgs_domains()
    print("Termino Extraccion de Organizaciones, tags y domains")

def Field_history(fecha, ids): 
    """
    Se hara la extraccion completa de los audits por que no tiene forma de 
    filtrarse a traves del query. 

    Debe buscarse una forma adicional de poder extraer estos datos. 
    """
    print("Inicia Extraccion de Field History")
    instancia = Zendesk_support(table = "field_history", ids = ids, tipo = "partial")
    instancia.field_history()
    print("Termina Extraccion de Field history")
    print()

def Field_options(): 
    """
    La extraccion de field Options se hara completa. 
    """
    print("Inicia Extraccion de Field Options")
    instancia = Zendesk_support(table = "field_options") 
    instancia.field_options()
    print("Termina Extraccion de Field Options")
    print()

def Tag_history(fecha, ids):
    """
    Mismo problema que Fields History, se tiene que hacer 
    la extraccion completa de audits

    Es posible que se pueda hacer el merge de esta funcion con 
    la de field_history para poder extraer todo de una sola 
    peticion
    """ 
    print("Inicia Extraccion de Tag history")
    instancia = Zendesk_support(table = "tag_history", tipo = "partial", ids = ids)
    instancia.tag_history()
    print("Termina Extraccion de Tag History")
    print()

def Groups(): 
    print("Inicia Extraccion de Grupos")
    Zendesk_support(table = "groups")
    print("Termina Extraccion de Grupos")
    print()

def Group_Members(): 
    print("Comenzo Extraccion de miembros de grupo")
    Zendesk_support(table = "group_members")
    print("Termino extraccion de miembros de grupo")
    print()

def Comments(ids): 
    print("Comienza Extraccion de Comentarios")
    instancia = Zendesk_support(table= "comments", tipo = "partial", ids = ids)
    instancia.Comments()
    print("Termino Extraccion de Commentarios. Al Fin")
    print()

def hilos(): 
    
    print("Comenzo Extraccion de Tickets")
    instancia = Zendesk_support(table = "tickets", tipo = "partial", fecha = fecha)
    instancia.Tickets()
    instancia.Tickets_tags()
    print("Termino Extraccion de Tickets")
    ids = list(instancia.tickets_table["ticket_id"])

    users = threading.Thread(target   = Users, args= (fecha,))
    tickets = threading.Thread(target = Tickets, args= (fecha,))
    orgs = threading.Thread(target = Organizations, args = (fecha,))
    field_history = threading.Thread(target = Field_history, args=(fecha,ids,))
    tag_history = threading.Thread(target = Tag_history, args=(fecha, ids, ))
    field_options = threading.Thread(target = Field_options)
    groups = threading.Thread(target = Groups)
    group_members = threading.Thread(target = Group_Members)
    comments = threading.Thread(target = Comments, args = (ids, ))


    users.start()
    orgs.start()
    field_history.start()
    tag_history.start()
    field_options.start()
    groups.start()
    group_members.start()
    comments.start()



if __name__ == "__main__":

    t1 = datetime.now()
    print("Hora de Comienzo: ", str(t1))
    hilos()
    t2 = datetime.now()
    print("Hora de Termino", str(t2))
    print("Tiempo Total: ", str(t2 -t1))
