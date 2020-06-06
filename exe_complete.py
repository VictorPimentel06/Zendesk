from main import Zendesk_support
import threading


def Tickets(): 
    print("Comienza Extraccion de Tickets")
    instancia = Zendesk_support(table = "tickets", tipo = 'complete') 
    instancia.Tickets()
    print("Termino Extraccion de Tickets")
    print()

    print("Comienza extraccion de Ticket Tags")
    instancia.Tickets_tags()
    print("Termina Extraccion de Ticket Tags")
    print()

def Users(): 
    print()
    print("Comienza Extraccion de Usuarios")
    instancia = Zendesk_support(table = "users")
    instancia.Users()
    print("Termina Extraccion de Usuarios")
    print()

    print("Comienza Extraccion de User Tags")
    instancia.Users_tags()
    print("Termina Extraccion de User Tags")
    print()

def Orgs(): 
    
    print()
    print("Comienza Extraccion de Organizaciones")
    instancia = Zendesk_support(table = "orgs") 
    instancia.Orgs()
    print("Termina Extraccion de Organizaciones")
    print()

    print("Inicia extraccion de Org tags")
    instancia.Orgs_tags()
    print("Termina Extraccion de Org Tags")
    print()

    print("Inicia Extracciond e Orgs Domains")
    instancia.Orgs_domains()
    print("Termina Extraccion de Org Domains")
    print()

def Field_options(): 
    print("Inicia Extraccion de Field Options")
    instancia = Zendesk_support(table = "field_options") 
    instancia.field_options()
    print("Termina Extraccion de Field Options")
    print()

def Field_history(): 
    print("Inicia Extraccion de Field History")
    instancia = Zendesk_support(table = "field_history")
    instancia.field_history()
    print("Termina Extraccion de Field history")
    print()

def Tag_history(): 
    print("Inicia Extraccion de Tag history")
    instancia = Zendesk_support(table = "tag_history")
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

def Comments(): 
    print("Comienza Extraccion de Comentarios")
    instancia = Zendesk_support(table= "comments")
    instancia.Comments()
    print("Termino Extraccion de Commentarios. Al Fin")
    print()

def hilos (): 
    tickets = threading.Thread(target = Tickets)
    users = threading.Thread(target   = Users)
    orgs = threading.Thread(target    =  Orgs)
    field_options = threading.Thread(target= Field_options)
    field_history = threading.Thread(target = Field_history)
    tag_history = threading.Thread(target = Tag_history)
    groups = threading.Thread(target = Groups)
    group_members = threading.Thread(target = Group_Members)
    comments = threading.Thread(target = Comments)

    tickets.start()
    users.start()
    orgs.start()
    field_options.start()
    field_history.start()
    tag_history.start()
    groups.start()
    group_members.start()
    comments.start()
if __name__ == "__main__": 
    hilos()
