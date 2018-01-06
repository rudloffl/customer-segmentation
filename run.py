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

from modules import invoicemanager as invoicemanager
#from modules import customermanager as customermanager
#from modules import segmentationmanager as segmentationmanager

folders = {'invoice':'invoice', 'customer':'customer'}
miscfolders = {'backup':'backup', 'logs':'logs'}


class Foldermanagement():
    def __init__(self):
        self.today = invoicemng.gettoday()
        for keys, foldername in {**folders, **miscfolders}.items():
            path = os.path.join(os.getcwd(), foldername)
            if not os.path.exists(path):
                os.makedirs(path)
    
    def daemon(self):
        self.logwriter('Command', 'Daemon started')
        self.watchfolder(5)
    
    def watchfolder(self, waiting = 1):
        self.standby = True
        while self.standby:
            time.sleep(waiting)
            for key, foldername in folders.items():
                mypath = os.path.join(os.getcwd(), foldername )
                filenames = [f for f in listdir(mypath) if isfile(join(mypath, f)) and (f.endswith('.csv') or f.endswith('.txt'))]     
                if filenames != []:
                    #print(filenames, ' in ', key)
                    self.requestreatment(key, filenames[0])
    
    def logwriter(self, reqtype, message):
        #print(message, ' - ', reqtype)
        now = datetime.datetime.now()
        filename =  now.strftime('%Y-%m-%d -- %H-%M-%S') + ' - ' + reqtype + '.txt'
        with open(os.path.join(os.getcwd(), miscfolders['logs'], filename), 'w') as f: 
            f.write(message) 
        
    def requestreatment(self, reqtype, task):
        if task.endswith('.txt'):
            self.commandtodo(reqtype, task)
        elif reqtype == 'invoice':
            self.treatnewinvoice(task)
        elif reqtype == 'customer':
            self.treatnewcustomer(task)
    
    def commandtodo(self, reqtype, task):
        if task == 'kill.txt':
            file = os.path.join(os.getcwd(), reqtype, task)
            os.remove(file)
            self.logwriter
            self.logwriter('Command', 'Daemon Stopped\n' + invoicemng.saveinvoicedb(miscfolders['backup']))
            self.standby = False
        elif task == 'updateinvoicedb.txt':
            #to do
            pass
        elif task == 'updatecustomerdb.txt':
            #to do
            pass
        
    def treatnewinvoice(self, file):
        filepath = os.path.join(os.getcwd(), folders['invoice'], file)
        messagelog = invoicemng.loadcsv(filepath)
        os.remove(filepath)
        self.logwriter('Invoice', messagelog)
        
            
    
if __name__ == "__main__":
    invoicemng = invoicemanager.InvoiceMng(miscfolders['backup'])

    foldermng = Foldermanagement()

    
    #we start the daemon
    foldermng.daemon()