from mysql.connector.cursor import MySQLCursor
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine

iddist = True #Crear Id's distintos para cada tabla (El tiempo de ejecución aumenta demasiado)

db = mysql.connector.connect(
    database="practica_6",
    host="localhost",
    user="root",
    password="root",
    port= '3306'
)
print(db)
mycursor = db.cursor()
dtb = create_engine("mysql+pymysql://" + 'root' + ":" + 'root' + "@" + 'localhost:3306'+ "/" + 'practica_6')

def Eliminar_Datos():
    print("Eliminando tablas y vistas....")
    mycursor.execute("drop table if exists hechos")
    mycursor.execute("drop table if exists tiempo_hechos")
    mycursor.execute("drop table if exists inicio_carpeta")
    mycursor.execute("drop table if exists fiscalia")
    mycursor.execute("drop table if exists locacion")

def Crear_id(string,fs):
    aux=[]
    for i in range(len(fs)):
        aux.append(string + str(fs[i]))
    id = pd.DataFrame()
    id['id'] = aux
    return id

def Leer_Archivo():
    fs="Dataset Reducido.csv"
    print("Cargando datos del archivo " + fs)
    archivo = pd.read_csv(fs)
    print("Extracion de datos completada")
    return archivo

def Tabla_Tiempohechos(archivo):
    fs = pd.DataFrame()
    if(iddist):
        id = Crear_id("H",archivo['id'])
        fs['id'] = id['id']
    else:
        fs['id'] = archivo['id']
    fs['fecha_hechos'] = archivo['fecha_hechos'] 
    fs['mes_hechos'] = archivo['mes_hechos'] 
    fs['ao_hechos'] = archivo['ao_hechos'] 
    print("Creando tabla de Tiempo_hechos...")
    fs.to_sql(con=dtb, name='tiempo_hechos', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return id

def Tabla_InicioCarp(archivo):
    fs = pd.DataFrame()
    if(iddist):
        id = Crear_id("IC",archivo['id'])
        fs['id'] = id['id']
    else:
        fs['id'] = archivo['id']
    fs['fecha_inicio'] = archivo['fecha_inicio'] 
    fs['mes_inicio'] = archivo['mes_inicio'] 
    fs['ao_inicio'] = archivo['ao_inicio'] 
    print("Creando tabla de Incio_carpeta...")
    fs.to_sql(con=dtb, name='inicio_carpeta', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return id
    

def Tabla_Fiscalia(archivo):
    fs = pd.DataFrame()
    if(iddist):
        id = Crear_id("F",archivo['id'])
        fs['id'] = id['id']
    else:
        fs['id'] = archivo['id']
    fs['fiscalia'] = archivo['fiscalia'] 
    fs['agencia'] = archivo['agencia'] 
    fs['unidad_investigacion'] = archivo['unidad_investigacion']
    print("Creando tabla de Fiscalia...")
    fs.to_sql(con=dtb, name='fiscalia', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return id

def Tabla_Locacion(archivo):
    fs = pd.DataFrame()
    if(iddist):
        id = Crear_id("L",archivo['id'])
        fs['id'] = id['id']
    else:
        fs['id'] = archivo['id']
    fs['colonia_hechos'] = archivo['colonia_hechos'] 
    fs['alcaldia_hechos'] = archivo['alcaldia_hechos'] 
    fs['calle_hechos'] = archivo['calle_hechos']
    fs['calle_hechos2'] = archivo['calle_hechos2']
    fs['longitud'] = archivo['longitud']
    fs['latitud'] = archivo['latitud']
    fs['geopoint'] = archivo['geopoint']
    print("Creando tabla de Locacion...")
    fs.to_sql(con=dtb, name='locacion', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return id

def Tabla_Hechos(archivo, idH, idIC, idF, idL):
    fs = pd.DataFrame()
    if(iddist):
        fs['idHechos'] = idH['id']
        fs['idInicio'] = idIC['id']
        fs['idFiscalia'] = idF['id']
        fs['idLocacion'] = idL['id']
    else:
        fs['idHechos'] = archivo['id']
        fs['idInicio'] = archivo['id']
        fs['idFiscalia'] = archivo['id']
        fs['idLocacion'] = archivo['id']
    fs['delito'] = archivo['delito']
    fs['categoria_delito'] = archivo['categoria_delito']
    print("Creando tabla de Hechos...")
    fs.to_sql(con=dtb, name='hechos', if_exists='replace', index=False)
    print("Tabla creada con exito")

def Formato_Tablas():
    print("Creando llaves de las tablas....")
    mycursor.execute("alter table tiempo_hechos change id id varchar(10) not null")
    mycursor.execute("alter table inicio_carpeta change id id varchar(10) not null")
    mycursor.execute("alter table fiscalia change id id varchar(10) not null")
    mycursor.execute("alter table locacion change id id varchar(10) not null")
    mycursor.execute("alter table hechos change idHechos idHechos varchar(10) not null")
    mycursor.execute("alter table hechos change idInicio idInicio varchar(10) not null")
    mycursor.execute("alter table hechos change idFiscalia idFiscalia varchar(10) not null")
    mycursor.execute("alter table hechos change idLocacion idLocacion varchar(10) not null")
    mycursor.execute("alter table tiempo_hechos add primary key (id);")
    mycursor.execute("alter table inicio_carpeta add primary key (id);")
    mycursor.execute("alter table fiscalia add primary key (id);")
    mycursor.execute("alter table locacion add primary key (id);")
    mycursor.execute("alter table hechos ADD FOREIGN KEY (idHechos) REFERENCES tiempo_hechos(id), add FOREIGN KEY (idInicio) REFERENCES inicio_carpeta(id), add FOREIGN KEY (idFiscalia) REFERENCES Fiscalia(id), add FOREIGN KEY (idLocacion) REFERENCES Locacion(id)")

Eliminar_Datos()
Archivo = Leer_Archivo()
idH = Tabla_Tiempohechos(Archivo)
idIC = Tabla_InicioCarp(Archivo)
idF = Tabla_Fiscalia(Archivo)
idL = Tabla_Locacion(Archivo)
Tabla_Hechos(Archivo, idH, idIC, idF, idL)
Formato_Tablas()
print("Todo Listo")
