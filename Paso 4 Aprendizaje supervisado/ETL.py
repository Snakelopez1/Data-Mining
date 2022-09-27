from re import split
import pandas as pd
import numpy as np
import mysql.connector
from pandas.core.frame import DataFrame
from sqlalchemy import create_engine

db = mysql.connector.connect(
    database="practica_8",
    host="localhost",
    user="root",
    password="root",
    port= '3306'
)
print(db)
mycursor = db.cursor()
dtb = create_engine("mysql+pymysql://" + 'root' + ":" + 'root' + "@" + 'localhost:3306'+ "/" + 'practica_8')

def Preparar_Datos():
    fs1="Datos_Entrenamiento.csv"
    fs2="Datos_Prueba.csv"
    print("Cargando datos del archivo " + fs1)
    archivo1 = pd.read_csv(fs1)
    archivo1.to_sql(con=dtb, name='datos_entrenamiento', if_exists='replace', index=False)
    print("Cargando datos del archivo " + fs2)
    archivo2 = pd.read_csv(fs2)
    archivo2.to_sql(con=dtb, name='datos_prueba', if_exists='replace', index=False)

Preparar_Datos()
print("Todo Listo")