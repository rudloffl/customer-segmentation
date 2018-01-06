#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 14:55:56 2018

@author: cricket
"""

import pandas as pd
import numpy as np
from scipy import stats
import math
from dateutil.parser import parse
from datetime import datetime, date, time
import calendar
from datetime import timedelta
import os

class InvoiceMng():
    def __init__(self, location):
        try:
            self.loadinvoicedb(location)
            self.message = '====== invoicedb loaded from backup ======\n'
        except:
            self.dataset = pd.DataFrame(columns = ['CustomerID'])
            self.dataset.index.name = 'InvoiceNo'
            self.pricelist = pd.DataFrame(columns = ['UnitPrice'])
            self.message = '====== no existing invoicedb existing, must be created ======\n'
        self.prices = pd.DataFrame()
        self.today = datetime(2010,1,1,0,0)
    
    def loadcsv(self, filename):
        self.message = '====== NEW INVOICE ADDITIONS ======\n'
        loadingfailure = False
        try:
            self.temp = pd.read_csv(filename, index_col = 'Unnamed: 0', parse_dates = ['InvoiceDate', ])
            self.temp = self.temp.sort_values('InvoiceDate')
            self.message += 'file loaded : {} \n'.format(filename)
            print('OK')
        except:
            self.message += 'Something bad occured during loading sequence :-(\n'
            loadingfailure = True
            print('KO')

        try:
            self.cleancsv()
        except:
            self.message += 'Something bad occured during cleaning sequence :-(\n'
            loadingfailure = True
        
        try:
            self.verifycsv()
        except:
            self.message += 'Something bad occured during verification sequence :-(\n'
            loadingfailure = True
        
        #try:
        self.datapreparation()
        #except:
        #    self.message += 'Something bad occured during preparation sequence :-(\n'
        #    loadingfailure = True        
        
        try:
            if not loadingfailure:
                self.addorders()
        except:
            self.message += 'Something bad occured during assembly sequence :-(\n'
        
        return self.message
    
    def cleancsv(self):
        self.message += '==== CSV file cleaned START ====\n'
        #We mark the cancelled invoices
        self.temp['Cancelled'] = self.temp['InvoiceNo'].apply(lambda x: 1 if str(x).startswith('C') else 0)
        self.message += '== Canceled order flagged ==\n'
        
        #We keep only the info from UK
        self.temp = self.temp[self.temp['Country'] == 'United Kingdom']
        self.message += '== Only orders from UK ==\n'
        
        #We drop the duplicates
        self.temp = self.temp.drop_duplicates()
        self.message += '== Dupplicates suppression ==\n'
        
        #We drop the fields with no customer ID
        self.temp = self.temp.dropna(axis = 0, how = 'any', subset = ['CustomerID'])
        self.message += '== Only orders with CustomerID ==\n'

        #drop all the that are extra fees
        extrafeereasons = ['POST', 'DOT', 'C2', 'CRUK', 'M', 'BANK CHARGES', 'PADS', 'D']
        for todrop in extrafeereasons:
            self.temp = self.temp[self.temp['StockCode'] != todrop]
        self.message += '== All the administrative fees dropped ==\n'
        
        #No free stuff in the dataset, must cost at least 1 penny
        self.temp = self.temp[self.temp['UnitPrice'] >= 0.01]
        self.message += '== No free stuff ! ==\n'
        
        self.message += '==== CSV file cleaned DONE ====\n'
    
    def verifycsv(self):
        #We verify that the order number is not already in the dataset
        invoicetemp = set(self.temp['InvoiceNo'])
        invoicedataset = set(self.dataset.reset_index()['InvoiceNo'])
        dupplicates = invoicetemp.intersection(invoicedataset)
        for dupplicate in dupplicates:
            self.temp = self.temp[self.temp['InvoiceNo'] != dupplicate]
            self.message += '== The order number {} already in the dataset==\n'.format(dupplicate)
    
    def datapreparation(self):
        #Totat per item
        self.temp['TotalItem'] = self.temp['UnitPrice'] * self.temp['Quantity']
        
        #we Make sure all the stockitems are in the pricelist
        self.pricelistupdate()
        
        #initial price calculation
        initpricedict = self.pricelist['UnitPrice'].to_dict()
        self.temp['Initprice'] = self.temp['StockCode'].apply(lambda x : initpricedict[x])
        self.temp['ItemDiscount'] = self.temp['Initprice'] / self.temp['UnitPrice']
        self.temp['TotalItemInit'] = self.temp['Initprice'] * self.temp['Quantity']
        
        #We split the unit prices by quantile
        pricedictquant = self.pricelist['QuantUnitPrice'].to_dict()
        self.temp['QuantUnitPrice'] = self.temp['StockCode'].apply(lambda x: pricedictquant.get(x))
    
        #Savings calculation
        self.temp['Savings'] = self.temp['TotalItem'] - self.temp['TotalItemInit']
        
        #quant unit price vectorization - dollars
        quant = self.temp.pivot(columns='QuantUnitPrice', values='TotalItem').fillna(0)
        new_names = [(i, 'QuantUnitPrice_{:02d}'.format(i)) for i in quant.columns.values]
        quant.rename(columns = dict(new_names), inplace=True)
        self.temp = self.temp.merge(quant, how='inner', left_index=True, right_index=True)
    
        #quant unit price savings vectorization - savings
        quant = self.temp.pivot(columns='QuantUnitPrice', values='Savings').fillna(0)
        new_names = [(i, 'QuantUnitSavings_{:02d}'.format(i)) for i in quant.columns.values]
        quant.rename(columns = dict(new_names), inplace=True)
        quant.head()
        self.temp = self.temp.merge(quant, how='inner', left_index=True, right_index=True)
        
        #Aggregation calculation
        def my_agg(x):
            aggcalcul = {
                'InvoiceDate': x['InvoiceDate'].min(),
                'TotalInvoice': x['TotalItem'].sum(),
                'TotalInvoiceInit': x['TotalItemInit'].sum(),
                'CustomerID':  x['CustomerID'].min(),
                'TotalSavings': x['Savings'].sum(),
                }
            return pd.Series(aggcalcul, index=aggcalcul.keys())
        self.tempagg = self.temp.groupby('InvoiceNo').apply(my_agg).fillna(0)
        
    
    def updatetoday(self):
        pass
    
    def gettoday(self):
        return self.today
    
    def addorders(self):
        self.dataset = pd.concat([self.dataset, self.tempagg])
        self.message += '==== New orders added to the main database ====\n'
    
    def updateorders(self):
        pass
    
    def pricelistupdate(self):
        classes = 10
        divider = 100/classes
        tempprice = self.temp[['UnitPrice', 'StockCode']].groupby('StockCode').first()
        print(tempprice)
        print(set(tempprice.index))
        print(set(self.pricelist.index))
        dupplicates = set(tempprice.index).intersection(set(self.pricelist.index))
        print('dupplicates -->', dupplicates)
        if len(dupplicates)==0:
            self.pricelist = pd.concat([self.pricelist, tempprice])
            print('no mask')
        else:
            mask = tempprice.index.isin(list(dupplicates))
            print('mask')
            self.pricelist = pd.concat([self.pricelist, tempprice.loc[np.logical_not(mask)]])
        self.pricelist['QuantUnitPrice'] = self.pricelist['UnitPrice'].apply(lambda x: math.ceil(stats.percentileofscore(self.pricelist['UnitPrice'],x, kind = 'mean')/divider))
        print(self.pricelist)
    
    def loadinvoicedb(self, location):
        self.dataset = pd.read_csv(os.path.join(os.getcwd(), location, 'invoicedb.csv'), index = 'Unnamed: 0')
        self.pricelist = pd.read_csv(os.path.join(os.getcwd(), location, 'pricelist.csv'), index = 'Unnamed: 0')
    
    def saveinvoicedb(self, location):
        self.dataset.to_csv(os.path.join(os.getcwd(), location, 'invoicedb.csv'))
        self.pricelist.to_csv(os.path.join(os.getcwd(), location, 'pricelist.csv'))
        self.message = '==== Invoicedb saved ====\n'
        return self.message
    
    def getinvoicedb(self, fromdate = None, todate = None, customers = None):
        pass