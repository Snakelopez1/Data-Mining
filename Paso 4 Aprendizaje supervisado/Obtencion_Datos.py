from re import split
import pandas as pd
import numpy as np
import mysql.connector
from pandas.core.frame import DataFrame
import datetime

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        port='3306',
        database="Practica_7"
    )
print(mydb)

def Get_Month(a):
    if(a == "Enero"):
        return 1
    elif(a == "Febrero"):
        return 2
    elif(a == "Marzo"):
        return 3
    elif(a == "Abril"):
        return 4
    elif(a == "Mayo"):
        return 5
    elif(a == "Junio"):
        return 6
    elif(a == "Julio"):
        return 7
    elif(a == "Agosto"):
        return 8
    elif(a == "Septiembre"):
        return 9
    elif(a == "Octubre"):
        return 10
    elif(a == "Noviembre"):
        return 11
    else:
        return 12

def Preparar_Datos():
    aux_d = []
    aux_m = []
    db = mydb.cursor()
    print("Extrayendo datos de Base de datos...")
    db.execute("SELECT * from categoria_delito;")
    df = db.fetchall()
    C = pd.DataFrame(df,columns = ['id_CAT','categoria_delito'])
    aux = C['id_CAT'].values
    for i in range(len(C)):
        aux_d.append(aux[i].split('C'))
        aux_m.append(aux_d[i][1])
    CAT = pd.DataFrame()
    CAT['idCAT'] = aux_m
    CAT['categoria_delito'] = C['categoria_delito']
    db.execute("SELECT idCAT, DiaSemana_hechos, mes_hechos, count(delito) as 'Delitos' FROM practica_7.hechos_tiempo_locacion where alcaldia_hechos = 'GUSTAVO A MADERO' group by idCAT, DiaSemana_hechos, mes_hechos order by mes_hechos, DiaSemana_hechos;")
    df = db.fetchall()
    DT = pd.DataFrame(df,columns = ['id_CAT','DiaSemana_hechos','Mes_hechos','No.Delitos'])
    datset = pd.DataFrame()
    aux = DT['id_CAT'].values
    aux_d = []
    aux_m = []
    for i in range(len(DT)):
        aux_d.append(aux[i].split('C'))
        aux_m.append(aux_d[i][1])
    datset['idCat'] = aux_m
    datset['DiaSem_hechos'] = DT['DiaSemana_hechos']
    aux = DT['Mes_hechos'].values
    aux_m = []
    for i in range(len(DT)):
        aux_m.append(Get_Month(aux[i]))
    datset['Mes_hechos'] = aux_m
    datset['NoDelitos'] = DT['No.Delitos']
    datset.to_csv('Datos_completos.csv' ,index=False)
    tam = int(len(datset)*.8)
    tam2 = int(len(datset)*.19)
    tam3 = tam + tam2 + 1
    Dta80 = datset.iloc[:tam,:]
    Dta19 = datset.iloc[tam:tam3,:]
    Dta01 = datset.iloc[tam3:,:]
    print("Creando Archivos...")
    Dta80.to_csv('Datos_Entrenamiento.csv',index=False)
    Dta19.to_csv('Datos_Prueba.csv',index=False)
    Dta01.to_csv('Datos_Restantes.csv',index=False)
    CAT.to_csv('Catalogo_Delitos.csv',index=False)
Preparar_Datos()
print("Todo Listo")