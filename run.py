#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 14:55:01 2018

@author: cricket
"""

from os import listdir
from os.path import isfile, join
import os
import time
import datetime

import numpy as np
import pandas as pd

from modules import invoicemanager as Invoicemanager
from modules import customermanager as Customermanager
from modules import segmentationmanager as Segmentationmanager

folders = {'invoice':'invoice', 'customer':'customer'}
miscfolders = {'backup':'backup', 'logs':'logs'}

class Foldermanagement():
    """Gathers the features to watch and execute the different requests"""
    def __init__(self):
        """Will initiate the class and create the needed folders"""
        self.today = invoicemng.gettoday()
        self.standby = False
        for foldername in {**folders, **miscfolders}.values():
            path = os.path.join(os.getcwd(), foldername)
            if not os.path.exists(path):
                os.makedirs(path)

    def daemon(self):
        """Used to start the daemon"""
        print('Daemon Started')
        self.logwriter('Command', 'Daemon started')
        self.watchfolder()

    def watchfolder(self, waiting=1):
        """Will watch the selected folder for new orders"""
        self.standby = True
        while self.standby:
            time.sleep(waiting)
            for key, foldername in folders.items():
                mypath = os.path.join(os.getcwd(), foldername)
                filenames = [f for f in listdir(mypath) if isfile(join(mypath, f)) \
                             and (f.endswith('.csv') or f.endswith('.txt'))]
                if filenames != []:
                    print(filenames, ' in ', key)
                    self.requestreatment(key, filenames[0])

    def logwriter(self, reqtype, message):
        """Will log the operation in txt files"""
        now = datetime.datetime.now()
        filename = now.strftime('%Y-%m-%d -- %H-%M-%S') + ' - ' + reqtype + '.txt'
        with open(os.path.join(os.getcwd(), miscfolders['logs'], filename), 'w') as f:
            f.write(message)

    def requestreatment(self, reqtype, task):
        """Will treat and coordinate orders found in the different folders"""
        if reqtype == 'invoice':
            self.treatnewinvoice(reqtype, task)
        elif reqtype == 'customer':
            self.commandtodo(reqtype, task)

    def commandtodo(self, reqtype, task):
        """Will execute the detected for customers"""
        #print(task)
        if task == 'kill.txt':
            messageinvoice = invoicemng.saveinvoicedb(miscfolders['backup'])
            messagesegmentation = segmentationmanager.savexgboost(miscfolders['backup'])
            self.logwriter('Command', 'Daemon Stopped\n' + messageinvoice + messagesegmentation)
            self.standby = False
        elif task.startswith('xgb-'):
            customerlist = pd.read_csv(task, index_col=0, header=None)[1].values
            self.customerclassification(methodcalcfile='xgb', customers=customerlist,
                                        reqtype=reqtype, task=task)
        elif task == 'stat.txt':
            self.customerclassification(methodcalcfile='stat', customers='All')
        elif task == 'train.txt':
            self.trainingXG(reqtype, task)
        file = os.path.join(os.getcwd(), reqtype, task)
        os.remove(file)

    def treatnewinvoice(self, reqtype, file):
        """Will treat new CSV invoices"""
        filepath = os.path.join(os.getcwd(), folders[reqtype], file)
        messagelog = invoicemng.loadcsv(filepath)
        os.remove(filepath)
        self.logwriter('Invoice', messagelog)

    def customerclassification(self, methodcalcfile='stat', customers='All',\
                               reqtype=None, task=None):
        """Will predict or calculate the scores for the customers"""
        if methodcalcfile == 'stat':
            start = time.time()
            dataframe = invoicemng.getinvoicedb(monthcovered=5)
            scores = customermng.customerclassstat(dataframe)
            end = time.time()
            timer = '\nResponse returned in {} sec\n'.format(end - start)
            date = invoicemng.gettoday().strftime('%Y-%m-%d -- %H-%M-%S')
            tableau = self.scoresformatting(scores)
            messagelog = 'Scores valid for {}\n\n'.format(date) + tableau + timer
            self.logwriter('stat', messagelog)
        elif methodcalcfile == 'xgb':
            if segmentationmanager.trained:
                pathfille = os.path.join(os.getcwd(), folders['customer'], task)
                start = time.time()
                customers = pd.read_csv(pathfille, index_col=0, header=None)[1].values
                invoice = invoicemng.getinvoicedb(customers=customers, monthcovered=5)
                customerdb = customermng.getdatasetpredict(invoice)
                scores, date = segmentationmanager.predict(customerdb)
                end = time.time()
                timer = '\nResponse returned in {} sec\n'.format(end - start)
                tableau = self.scoresformatting(scores)
                missing = [x for x in customers if x not in scores.index]

                date = date.strftime('%Y-%m-%d')
                messagelog = 'Score prediction form XGBoost\n'
                messagelog += 'Training date {}\n'.format(date)
                messagelog += tableau
                if missing != []:
                    messagelog += 'Customers missing from database : {}\n'.format(missing)
                messagelog += timer
                self.logwriter('XGBoost', messagelog)
            else:
                self.logwriter('error', 'The model is not trained\n')

    def trainingXG(self, reqtype, task):
        """Will train the RFC model"""
        start = time.time()
        invoicedb = invoicemng.getinvoicedb(monthcovered=5)
        dataset = customermng.getdatasetrmf(invoicedb)
        messagelog = segmentationmanager.train(dataset, quicktraining=True)
        end = time.time()
        timer = '\nCalculation done in {} sec\n'.format(end - start)
        messagelog += timer
        self.logwriter('Command', messagelog)

    def scoresformatting(self, scores):
        """Will format the dataframe to a string print ready"""
        toreturn = ' CustomerID | R | F | M | D | C \n'
        separator = '------------+---+---+---+---+---\n'
        for _, customer in scores.reset_index().iterrows():
            toreturn += separator
            customerid = customer.CustomerID
            r = int(customer.R)
            f = int(customer.F)
            m = int(customer.M)
            d = int(customer.D)
            c = int(customer.C)
            toreturn += '  {}   | {} | {} | {} | {} | {} \n'.format(customerid, r, f, m, d, c)
        return toreturn


if __name__ == "__main__":
    invoicemng = Invoicemanager.InvoiceMng(miscfolders['backup'])
    customermng = Customermanager.Customermanager()
    segmentationmanager = Segmentationmanager.Segmentationmanager(miscfolders['backup'])

    foldermng = Foldermanagement()

    #we start the daemon
    foldermng.daemon()
    