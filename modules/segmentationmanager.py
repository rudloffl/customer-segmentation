#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 14:57:04 2018

@author: cricket
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime
import pickle
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import make_scorer
from sklearn.model_selection import StratifiedKFold
from sklearn import preprocessing
from sklearn.metrics import f1_score
import xgboost as xgb

class Segmentationmanager():
    BACKUPNAME = 'XGBOOST'
    
    def __init__(self, location):
        self.backuploc = location
        try:
            self.loadxgboost(location)
            self.message = '====== invoicedb loaded from backup ======\n'
        except:
            self.today = datetime(1950, 1, 1, 0, 0)
            self.trained = False
            self.message = '====== no existing invoicedb existing, must be created ======\n'
            self.xgboostdict = {}
            self.scaler = preprocessing.StandardScaler()


    
    def train(self, dataset):
        """Trains the model"""
        #data split per criteria
        criterias = ['M', 'F', 'R', 'D', 'C']
        testingsize = 0.28
        testlist = []
        for criteria in criterias:
            df = dataset[[criteria,'CustomerID']].groupby('CustomerID', as_index = False).nth(-1)
            df = df[[criteria,'CustomerID']].groupby(criteria)
            tempdict = df['CustomerID'].apply(set).to_dict()
            for keys, values in tempdict.items():
                rows = random.sample(list(values), int(testingsize * len(values)/len(criterias)))
                #print(int(testingsize * len(values)/len(criterias)),len([x for x in rows if x not in testlist]) , len(values))
                testlist.extend([x for x in rows if x not in testlist])
        
        #labels definition
        target = ['R', 'F', 'M', 'D', 'C', 'RFMDC']
        variables = [x for x in dataset.columns.values if x not in ['R', 'F', 'M', 'D', 'C', 'RFMDC', 'CustomerID','LastInvoice' ,'Timestamp']]
        
        #testing and training set creation
        mask = dataset['CustomerID'].isin([x for x in dataset['CustomerID'].unique() if x not in testlist])
        trainingset = dataset.loc[mask]        
        mask = dataset['CustomerID'].isin(testlist)
        testingset = dataset.loc[mask]
        
        #Sets creation
        Xtest = testingset[variables]
        ytest = testingset[target]
        Xtrain = trainingset[variables]
        ytrain = trainingset[target]
        
        #Data standardisation
        self.scaler = preprocessing.StandardScaler().fit(Xtrain)
        X_train_scaled = self.scaler.transform(Xtrain)
        X_test_scaled = self.scaler.transform(Xtest)
    
        #Training for all the parameters XgBoost
        self.xgboostdict = {}
        parameterlist = ['R', 'F', 'M', 'D', 'C']
        scorer = make_scorer(f1_score, average = 'micro')
        skf = StratifiedKFold(n_splits=3)
        ind_params ={ 'n_estimators':30, 'learning_rate':0.1, 'nthread':3, 'objective': 'multi:softmax'}
        featureimportancedictXGBoost = {}
        
        for parametertoclass in parameterlist:
            #Gridsearch creation - XGBoostingClassifier - Step 1
            step1 = {**ind_params}
            parameters = {'max_depth': range(8,13,2), 'min_child_weight':range(1,6,2) }
            xgbc1 = xgb.XGBClassifier(**step1)
            xgbcclf1 = GridSearchCV(xgbc1, parameters, scoring = scorer, n_jobs = 3, cv = skf, verbose = 5, return_train_score=True)
            xgbcclf1.fit(X_train_scaled, ytrain[parametertoclass])
            #xgbcclf1.score(X_test_scaled, ytest[parametertoclass])
            
            #Gridsearch creation - XGBoostingClassifier - Step 2
            step2 = {**xgbcclf1.best_params_, **step1}
            parameters = {'gamma':[i/10.0 for i in range(0,3)]}
            xgbc2 = xgb.XGBClassifier(**step2)
            xgbcclf2 = GridSearchCV(xgbc2, parameters, scoring = scorer, n_jobs = 3, cv = skf, verbose = 5, return_train_score=True)
            xgbcclf2.fit(X_train_scaled, ytrain[parametertoclass])
            #xgbcclf2.score(X_test_scaled, ytest[parametertoclass])
            
            #Gridsearch creation - XGBoostingClassifier - Step 3
            step3 = {**xgbcclf2.best_params_, **step2}
            parameters = {'subsample':[i/10.0 for i in range(6,10)], 'colsample_bytree':[i/10.0 for i in range(6,10)]}
            xgbc3 = xgb.XGBClassifier(**step3)
            xgbcclf3 = GridSearchCV(xgbc3, parameters, scoring = scorer, n_jobs = 3, cv = skf, verbose = 5, return_train_score=True)
            xgbcclf3.fit(X_train_scaled, ytrain[parametertoclass])
            #xgbcclf3.score(X_test_scaled, ytest[parametertoclass])
            
            #Gridsearch creation - XGBoostingClassifier - Step 4
            step4 = {**xgbcclf3.best_params_, **step3}
            parameters = {'learning_rate':[0.01, 0.05, 0.1]}
            xgbc4 = xgb.XGBClassifier(**step4)
            xgbcclf4 = GridSearchCV(xgbc4, parameters, scoring = scorer, n_jobs = 3, cv = skf, verbose = 5, return_train_score=True)
            xgbcclf4.fit(X_train_scaled, ytrain[parametertoclass])
            score = xgbcclf4.score(X_test_scaled, ytest[parametertoclass])
            
            XGBClass = xgb.XGBClassifier(**{**step4, **xgbcclf4.best_params_})
            xgbClass = XGBClass.fit(X_train_scaled, ytrain[parametertoclass])
            featureimportancedictXGBoost[parametertoclass] = xgbClass.feature_importances_
            
            print(parametertoclass , score)
            self.xgboostdict[parametertoclass] = XGBClass
    
    def predict(self, customer):
        """Predicts the scores for the listed customers"""
        pass
    
    def gettoday(self):
        """Will return the latest date from the orders"""
        return self.today

    def loadxgboost(self):
        """loads the XGBoost dictionnary from the backup folder"""
        pathname = os.path.join(os.getcwd(), self.backuploc, '{}.pkl'.format(BACKUPNAME))
        def load_obj(name):
            with open(pathname, 'rb') as f:
                return pickle.load(f)
        self.today = load_obj('today')
        self.xgboostdict = load_obj('xgboostdict')
        self.scaler = load_obj('scaler')
        
    def timestamping(self,dataset):
        """returns a timestamping on the dataset"""
        dataset['Weekday'] = dataset['Timestamp'].apply(lambda x: x.weekday())
        return dataset
        
    def savexgboost(self, location):
        """Saves  the XGBoost dictionnary from the backup folder"""
        pathname = os.path.join(os.getcwd(), self.backuploc, '{}.pkl'.format(BACKUPNAME))
        today = self.today
        xgboostdict = self.xgboostdict
        scaler = self.scaler
        with open(pathname, 'wb') as f:
            pickle.dump(today, f, pickle.HIGHEST_PROTOCOL)
            pickle.dump(xgboostdict, f, pickle.HIGHEST_PROTOCOL)
            pickle.dump(scaler, f, pickle.HIGHEST_PROTOCOL)




if __name__ == "__main__":
    segmentationmanager = Segmentationmanager()
    #Dicts loading
    def load_obj(name):
        with open('dict/' + name + '.pkl', 'rb') as f:
            return pickle.load(f)