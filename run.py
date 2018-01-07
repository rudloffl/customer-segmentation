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

from modules import invoicemanager as Invoicemanager
from modules import customermanager as Customermanager
#from modules import segmentationmanager as segmentationmanager

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
        self.logwriter('Command', 'Daemon started')
        self.watchfolder(5)

    def watchfolder(self, waiting=1):
        """Will watch the selected folder for new orders"""
        self.standby = True
        while self.standby:
            time.sleep(waiting)
            for key, foldername in folders.items():
                mypath = os.path.join(os.getcwd(), foldername)
                filenames = [f for f in listdir(mypath) if isfile(join(mypath, f)) and (f.endswith('.csv') or f.endswith('.txt'))]
                if filenames != []:
                    print(filenames, ' in ', key)
                    self.requestreatment(key, filenames[0])

    def logwriter(self, reqtype, message):
        """Will log the operation in txt files"""
        #print(message, ' - ', reqtype)
        now = datetime.datetime.now()
        filename = now.strftime('%Y-%m-%d -- %H-%M-%S') + ' - ' + reqtype + '.txt'
        with open(os.path.join(os.getcwd(), miscfolders['logs'], filename), 'w') as f:
            f.write(message)

    def requestreatment(self, reqtype, task):
        """Will treat and coordinate orders found in the different files"""
        if task.endswith('.txt'):
            if task.startswith('rfm'):
                self.customerclassification(methodcalcfile='rfm', customers=task)
            elif task.startswith('stat'):
                self.customerclassification(methodcalcfile='stat', customers='All')
            else:
                self.commandtodo(reqtype, task)
        elif reqtype == 'invoice':
            self.treatnewinvoice(task)

    def commandtodo(self, reqtype, task):
        """Will execute the detected commands"""
        print(task)
        if task == 'kill.txt':
            file = os.path.join(os.getcwd(), reqtype, task)
            os.remove(file)
            self.logwriter('Command', 'Daemon Stopped\n' + invoicemng.saveinvoicedb(miscfolders['backup']))
            self.standby = False
        elif task == 'updateinvoicedb.txt':
            #to do
            pass
        elif task == 'updatecustomerdb.txt':
            #to do
            pass

    def treatnewinvoice(self, file):
        """Will treat new CSV invoices"""
        filepath = os.path.join(os.getcwd(), folders['invoice'], file)
        messagelog = invoicemng.loadcsv(filepath)
        os.remove(filepath)
        self.logwriter('Invoice', messagelog)

    def customerclassification(self, methodcalcfile='stat', customers='All'):
        """Will predict or calculate the scores for the customers"""
        if methodcalcfile == 'stat':
            dataframe = invoicemng.getinvoicedb()
            scores = customermng.customerclassstat(dataframe)
            print(scores)


if __name__ == "__main__":
    invoicemng = Invoicemanager.InvoiceMng(miscfolders['backup'])
    customermng = Customermanager.Customermanager()
    foldermng = Foldermanagement()


    #we start the daemon
    foldermng.daemon()