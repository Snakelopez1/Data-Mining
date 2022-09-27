import pandas as pd
import numpy as np
import mysql.connector
from math import factorial
from pandas.core.frame import DataFrame
from sqlalchemy import create_engine


mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        port='3306',
        database="Practica_6"
    )
print(mydb)

mydb1 = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        port='3306',
        database="Practica_7"
    )

dtb = create_engine("mysql+pymysql://" + 'root' + ":" + 'root' + "@" + 'localhost:3306'+ "/" + 'practica_7')

def numero_combinaciones(m, n):
    return factorial(m) // (factorial(n) * factorial(m - n))

def Nivel_1():
    db = mydb.cursor()
    db.execute("SELECT categoria_delito FROM hechos group by categoria_delito")
    df = db.fetchall()
    categoria = pd.DataFrame()
    categoria['idCAT'] = ['C0','C1','C2','C3','C4','C5','C6','C7','C8','C9','C10','C11','C12','C13','C14','C15']
    categoria['categoria_delito'] = df
    print("Creando tabla de Categoria_Delito...")
    categoria.to_sql(con=dtb, name='categoria_delito', if_exists='replace', index=False)
    print("Tabla creada con exito")

    db = mydb.cursor()
    db.execute("SELECT idHechos, idLocacion, categoria_delito, delito FROM hechos")
    df = db.fetchall()
    hechos = pd.DataFrame(df,
                    columns=['idHechos', 'idLocacion', 'categoria_delito', 'delito'])
    print("Creando tabla de Hechos...")
    hechos.to_sql(con=dtb, name='hec', if_exists='replace', index=False)

    db1 = mydb1.cursor()
    db1.execute("SELECT categoria_delito.idCAT, hec.delito FROM hec JOIN categoria_delito ON hec.categoria_delito = categoria_delito.categoria_delito")
    df = db1.fetchall()
    aux = pd.DataFrame(df,
                    columns=['idCAT', 'delito'])
    hechos['idCAT'] = aux['idCAT']
    hechos.to_sql(con=dtb, name='hechos', if_exists='replace', index=False)
    print("Tabla creada con exito")
    hechos = hechos.drop(['categoria_delito'], axis=1)
    db1.execute("drop table if exists hec")
    db1.close()

    db = mydb.cursor()
    db.execute("SELECT * FROM locacion")
    df = db.fetchall()
    locacion = pd.DataFrame(df,
                    columns=['id', 'colonia_hechos', 'alcaldia_hechos', 'calle_hechos', 'calle_hechos2', 'longitud', 'latitud', 'geopoint'])
    print("Creando tabla de Locacion...")
    locacion.to_sql(con=dtb, name='locacion', if_exists='replace', index=False)
    print("Tabla creada con exito")

    db = mydb.cursor()
    db.execute("SELECT * FROM tiempo_hechos")
    df = db.fetchall()
    tiempo_hechos = pd.DataFrame(df,
                    columns=['id', 'fecha_hechos', 'DiaSemana_hechos', 'mes_hechos', 'ao_hechos'])
    print("Creando tabla de Tiempo_Hechos...")
    tiempo_hechos.to_sql(con=dtb, name='tiempo_hechos', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return hechos,categoria,tiempo_hechos,locacion

def Nivel_2(h, c, t, l):
    ht = pd.merge(h, t, left_on='idHechos', right_on='id')
    print("Creando tabla de Hechos_Tiempo...")
    ht.to_sql(con=dtb, name='hechos_tiempo', if_exists='replace', index=False)
    print("Tabla creada con exito")
    hl = pd.merge(h, l, left_on='idLocacion', right_on='id')
    print("Creando tabla de Hechos_Locacion...")
    hl.to_sql(con=dtb, name='hechos_locacion', if_exists='replace', index=False)
    print("Tabla creada con exito")
    hc = pd.merge(h, c, left_on='idCAT', right_on='idCAT')
    print("Creando tabla de Hechos_Categoria...")
    hc.to_sql(con=dtb, name='hechos_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")
    tl = pd.concat([t, l], axis=1)
    print("Creando tabla de Tiempo_Locacion...")
    tl.to_sql(con=dtb, name='tiempo_locacion', if_exists='replace', index=False)
    print("Tabla creada con exito")
    tc = pd.concat([t, c],axis=1)
    print("Creando tabla de Tiempo_Categoria...")
    tc.to_sql(con=dtb, name='tiempo_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")
    lc = pd.concat([l, c],axis=1)
    print("Creando tabla de Locacion_Categoria...")
    lc.to_sql(con=dtb, name='locacion_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return tl, tc, lc

def Nivel_3(h, t , tl, tc, lc):
    htl = pd.concat([h, tl], axis=1)
    print("Creando tabla de Hechos_Tiempo_Locacion...")
    htl.to_sql(con=dtb, name='hechos_tiempo_locacion', if_exists='replace', index=False)
    print("Tabla creada con exito")
    htc = pd.concat([h, tc], axis=1)
    print("Creando tabla de Hechos_Tiempo_Categoria...")
    htc.to_sql(con=dtb, name='hechos_tiempo_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")
    hlc= pd.concat([h, lc], axis=1)
    print("Creando tabla de Hechos_Locacion_Categoria...")
    hlc.to_sql(con=dtb, name='hechos_locacion_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")
    tlc = pd.concat([t, lc], axis=1)
    print("Creando tabla de Tiempo_Locacion_Categoria...")
    tlc.to_sql(con=dtb, name='tiempo_locacion_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")
    return htl

def Nivel_4(htl, c):
    htlf = pd.concat([htl, c], axis=1)
    print("Creando tabla de Hechos_Tiempo_Locacion_Categoria...")
    htlf.to_sql(con=dtb, name='hechos_tiempo_locacion_categoria', if_exists='replace', index=False)
    print("Tabla creada con exito")

def Cubos(h,t,l):
    hl = pd.merge(h, l, left_on='idLocacion', right_on='id')
    print("Creando Cubo A...")
    hl = hl.drop(['id'], axis=1)
    hl = hl.drop(['calle_hechos'], axis=1)
    hl = hl.drop(['calle_hechos2'], axis=1)
    hl = hl.drop(['geopoint'], axis=1)
    hl.to_sql(con=dtb, name='cubo_a', if_exists='replace', index=False)
    print("Cubo creado con exito")
    
    ht = pd.merge(h, t, left_on='idHechos', right_on='id')
    print("Creando Cubo B...")
    htl= pd.concat([ht, l], axis=1)
    htl = htl.drop(['id'], axis=1)
    htl = htl.drop(['calle_hechos'], axis=1)
    htl = htl.drop(['calle_hechos2'], axis=1)
    htl = htl.drop(['geopoint'], axis=1)
    htl.to_sql(con=dtb, name='cubo_b', if_exists='replace', index=False)
    print("Cubo creado con exito")

h,c,t,l = Nivel_1()
tl, tc, lc = Nivel_2(h,c,t,l)
htl = Nivel_3(h, t, tl, tc, lc)
Nivel_4(htl, c)
Cubos(h,t,l)