import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.svm import SVR
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import precision_score,accuracy_score
import numpy as np

dfTraining = pd.read_csv("Datos_Entrenamiento.csv")
dfTesting = pd.read_csv("Datos_Prueba.csv")

 
X_train = dfTraining[["idCat","DiaSem_hechos","Mes_hechos"]]
y_train=dfTraining.NoDelitos

X_testing = dfTesting[["idCat","DiaSem_hechos","Mes_hechos"]]
y_testing = dfTesting.NoDelitos


print("-------------------- Normal SVM -------------------------")

clf = SVR(C=1.0, epsilon=0.2)
clf.fit(X_train, y_train) 
SVR(C=1.0, cache_size=200, coef0=0.0,
  degree=3, gamma='auto', kernel='rbf',
  max_iter=-1,  shrinking=True,
  tol=0.001, verbose=False)
scores = cross_val_score(clf, X_train, y_train, cv = 10)
res1=clf.predict(X_testing)

print("---------------------------------------------")
index=0
for element in res1:
    error=(abs((element-y_testing[index]))/y_testing[index])*100
    print('Predicted Value: ', element, ' Real value: ', y_testing[index], " % Error: ", error)
    index=index+1
    
import csv
pathCsvfile="./outRealTesting1.csv"
with open(pathCsvfile, 'w', encoding='utf-8', newline='') as csvFile:
    fieldnames = ['predictedValue', 'RealValue','ErrorPercent']
    writer = csv.DictWriter(csvFile, fieldnames=fieldnames) 
    writer.writeheader() 
    index=0
    for element in res1:
        error=(abs((element-y_testing[index]))/y_testing[index])*100
        print('Predicted Value: ', element, ' Real value: ', y_testing[index], " % Error: ", error)
        writer.writerow({'predictedValue': "'"+format(element)+"'", 'RealValue': "'"+format(y_testing[index])+"'", 'ErrorPercent': "'"+format(error)+"'"})
        index=index+1
print("---------------------------------------------")


print("--------------------SVM Radial bases-------------------------")

parametersSVM = {"C":  [1,10,100, 1000, 10000,100000],
              "gamma": [0.1,0.01,0.001,0.0001,1,10,100]}

gs_clf = GridSearchCV(clf, parametersSVM, n_jobs=-1)
gs_clf = gs_clf.fit(X_train,y_train)
gs_clf.best_score_ 
rbf_svc_tunning = gs_clf.best_estimator_

y_svm2 = rbf_svc_tunning.fit(X_train, y_train)
score2=rbf_svc_tunning.score(X_train, y_train)
crossvalue = cross_val_score(rbf_svc_tunning, X_train, y_train, cv = 10)

res2=rbf_svc_tunning.predict(X_testing)
index=0
for element in res2:
    error=(abs((element-y_testing[index]))/y_testing[index])*100
    print('Predicted Value: ', element, ' Real value: ', y_testing[index], " % Error: ", error)
    index=index+1
print("---------------------------------------------")

import csv
pathCsvfile="./outRealTesting2.csv"
with open(pathCsvfile, 'w', encoding='utf-8', newline='') as csvFile:
    fieldnames = ['predictedValue', 'RealValue','ErrorPercent']
    writer = csv.DictWriter(csvFile, fieldnames=fieldnames) 
    writer.writeheader() 
    index=0
    for element in res2:
        error=(abs((element-y_testing[index]))/y_testing[index])*100
        print('Predicted Value: ', element, ' Real value: ', y_testing[index], " % Error: ", error)
        writer.writerow({'predictedValue': "'"+format(element)+"'", 'RealValue': "'"+format(y_testing[index])+"'", 'ErrorPercent': "'"+format(error)+"'"})
        index=index+1