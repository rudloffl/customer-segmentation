#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 14:55:56 2018

@author: cricket
"""
import os
import calendar
import math
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats
#from dateutil.parser import parse

#from datetime import timedelta


class InvoiceMng():
    """Will handle the new invoice from CSV and maintain a invoicedb file"""
    def __init__(self, location):
        """Initiate the classes, will try to load a possible backup or create
        empty files"""
        try:
            self.loadinvoicedb(location)
            self.message = '====== invoicedb loaded from backup ======\n'
        except:
            self.dataset = pd.DataFrame(columns=['CustomerID'])
            self.dataset.index.name = 'InvoiceNo'
            self.pricelist = pd.DataFrame(columns=['UnitPrice'])
            self.message = '====== no existing invoicedb existing, must be created ======\n'
        self.prices = pd.DataFrame()
        self.temp = pd.DataFrame()
        self.tempagg = pd.DataFrame()
        self.today = datetime(2010, 1, 1, 0, 0)
        self.message = ''

    def loadcsv(self, filename):
        """Will coordinate the loading and formating of the csv loaded"""
        self.message = '====== NEW INVOICE ADDITIONS ======\n'
        loadingfailure = False
        try:
            self.temp = pd.read_csv(filename, index_col='Unnamed: 0', parse_dates=['InvoiceDate', ])
            self.temp = self.temp.sort_values('InvoiceDate')
            self.message += 'file loaded : {} \n'.format(filename)
        except:
            self.message += '@@@ Something bad occured during loading sequence :-(\n'
            loadingfailure = True

        try:
            self.cleancsv()
        except:
            self.message += '@@@ Something bad occured during cleaning sequence :-(\n'
            loadingfailure = True

        try:
            self.verifycsv()
        except:
            self.message += '@@@ Something bad occured during verification sequence :-(\n'
            loadingfailure = True

        try:
            self.datapreparation()
        except:
            self.message += '@@@ Something bad occured during preparation sequence :-(\n'
            loadingfailure = True

        try:
            if not loadingfailure:
                self.addorders()
        except:
            self.message += '@@@ Something bad occured during assembly sequence :-(\n'

        try:
            self.updateorders()
        except:
            self.message += '@@@ Something bad occured during order separation calculation sequence :-(\n'

        self.updatetoday()
        self.message += 'Today defined as {}'.format(self.today.strftime('%Y-%m-%d -- %H-%M-%S'))

        return self.message

    def cleancsv(self):
        """Will clean the csv just loaded"""
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
        self.temp = self.temp.dropna(axis=0, how='any', subset=['CustomerID'])
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
        """Will make sure that no order already existing in the main database
        Will be added twice"""
        #We verify that the order number is not already in the dataset
        invoicetemp = set(self.temp['InvoiceNo'])
        invoicedataset = set(self.dataset.reset_index()['InvoiceNo'])
        #print('existing ', invoicedataset)
        dupplicates = invoicetemp.intersection(invoicedataset)
        #print('dupplicates ', dupplicates)
        for dupplicate in dupplicates:
            self.temp = self.temp[self.temp['InvoiceNo'] != dupplicate]
            self.message += '== The order number {} already in the dataset==\n'.format(dupplicate)

    def datapreparation(self):
        """Will do multiple calculation on the csv just loaded"""
        #Totat per item
        self.temp['TotalItem'] = self.temp['UnitPrice'] * self.temp['Quantity']

        #we Make sure all the stockitems are in the pricelist
        self.pricelistupdate()

        #initial price calculation
        initpricedict = self.pricelist['UnitPrice'].to_dict()
        self.temp['Initprice'] = self.temp['StockCode'].apply(lambda x: initpricedict[x])
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
        quant.rename(columns=dict(new_names), inplace=True)
        self.temp = self.temp.merge(quant, how='inner', left_index=True, right_index=True).fillna(0)

        #quant unit price savings vectorization - savings
        quant = self.temp.pivot(columns='QuantUnitPrice', values='Savings').fillna(0)
        new_names = [(i, 'QuantUnitSavings_{:02d}'.format(i)) for i in quant.columns.values]
        quant.rename(columns=dict(new_names), inplace=True)
        self.temp = self.temp.merge(quant, how='inner', left_index=True, right_index=True).fillna(0)

        #Amount cancelled
        self.temp['AmountCancelled'] = self.temp['Cancelled'] * self.temp['TotalItem']

        #Aggregation calculation
        def my_agg(x):
            """Aggregation feature used to calculate the invoicedb"""
            aggcalcul = {
                'InvoiceDate': x['InvoiceDate'].min(),
                'TotalInvoice': x['TotalItem'].sum(),
                'TotalInvoiceInit': x['TotalItemInit'].sum(),
                'CustomerID':  x['CustomerID'].min(),
                'TotalSavings': x['Savings'].sum(),
                'AmountCancelled' : x['AmountCancelled'].sum(),
                }
            return pd.Series(aggcalcul, index=aggcalcul.keys())
        self.tempagg = self.temp.groupby('InvoiceNo').apply(my_agg).fillna(0)

        #detail orders for invoicedb - QUANT UNIT PRICE
        detail = [x for x in self.temp.columns.values if x.startswith('QuantUnitPrice_')]
        detail.append('InvoiceNo')
        temp = self.temp[detail].groupby('InvoiceNo').sum()
        self.tempagg = self.tempagg.merge(temp, how='inner', left_index=True, right_index=True).fillna(0)

        #detail orders for invoicedb - QUANT UNIT SAVINGS
        detail = [x for x in self.temp.columns.values if x.startswith('QuantUnitSavings_')]
        detail.append('InvoiceNo')
        temp = self.temp[detail].groupby('InvoiceNo').sum()
        self.tempagg = self.tempagg.merge(temp, how='inner', left_index=True, right_index=True).fillna(0)

        #InvoiceDB discount
        self.tempagg['Discount'] = self.tempagg['TotalInvoice'] / self.tempagg['TotalInvoiceInit']

        #When the order has been placed during the day in pounds?
        def daysplit(x):
            """Will mark the days:
                1 for the morning,
                2 in the afternoon or
                3 for the night"""
            hour = x.hour
            if 6 < hour < 12:
                return 1
            elif 12 <= hour < 20:
                return 2
            return 3

        self.tempagg['Daytime'] = self.tempagg['InvoiceDate'].apply(daysplit)
        temp = self.tempagg.pivot(columns='Daytime', values='TotalInvoice').fillna(0)
        new_names = [(i, 'Daytime_Monetary_'+str(i)) for i in temp.columns.values]
        temp.rename(columns=dict(new_names), inplace=True)
        self.tempagg = self.tempagg.merge(temp, how='inner', left_index=True, right_index=True).fillna(0)

        #When the order has been placed during the week in pounds?
        def weeksplit(x):
            """Will return a string with the day number in the week"""
            day = x.weekday()
            return 'Weekday_{}_{}'.format(day, list(calendar.day_name)[day])
        self.tempagg['Weekday'] = self.tempagg['InvoiceDate'].apply(weeksplit)
        temp = self.tempagg.pivot(columns='Weekday', values='TotalInvoice').fillna(0)
        self.tempagg = self.tempagg.merge(temp, how='inner', left_index=True, right_index=True).fillna(0)

        #When the order has been placed during the month?
        def monthsplit(x):
            """Will return the month number"""
            month = x.month
            return 'Month_{:02d}'.format(month)
        self.tempagg['MonthOrder'] = self.tempagg['InvoiceDate'].apply(monthsplit)
        temp = self.tempagg.pivot(columns='MonthOrder', values='TotalInvoice').fillna(0)
        self.tempagg = self.tempagg.merge(temp, how='inner', left_index=True, right_index=True).fillna(0)

    def updatetoday(self):
        """Will calculate the latest day from the orders"""
        self.today = self.dataset['InvoiceDate'].max()

    def gettoday(self):
        """Will return the latest date from the orders"""
        return self.today

    def addorders(self):
        """Will assemble the temporary file treated with the existing invoice list"""
        self.dataset = pd.concat([self.dataset, self.tempagg])
        self.message += '==== New orders added to the main database ====\n'

    def updateorders(self):
        """Will update the day between orders calculation"""
        self.dataset = self.dataset.sort_values('InvoiceDate')
        self.dataset['Ordersep'] = self.dataset[['CustomerID', 'InvoiceDate']].groupby(['CustomerID']).InvoiceDate.apply(lambda x: x.diff()).fillna(0)
        self.dataset['Ordersep'] = self.dataset['Ordersep'].apply(lambda x: x.days)

    def pricelistupdate(self):
        """Will update the quartiles for the stockode in the pricelist"""
        classes = 5
        divider = 100/classes
        tempprice = self.temp[['UnitPrice', 'StockCode']].groupby('StockCode').first()
        dupplicates = set(tempprice.index).intersection(set(self.pricelist.index))
        if len(dupplicates) == 0:
            self.pricelist = pd.concat([self.pricelist, tempprice])
        else:
            mask = tempprice.index.isin(list(dupplicates))
            self.pricelist = pd.concat([self.pricelist, tempprice.loc[np.logical_not(mask)]])
        self.pricelist['QuantUnitPrice'] = self.pricelist['UnitPrice'].apply(lambda x: math.ceil(stats.percentileofscore(self.pricelist['UnitPrice'], x, kind='mean')/divider))

    def loadinvoicedb(self, location):
        """Will load an existing database from the backup folder"""
        self.dataset = pd.read_csv(os.path.join(os.getcwd(), location, 'invoicedb.csv'), index_col='InvoiceNo', parse_dates=['InvoiceDate'])
        self.pricelist = pd.read_csv(os.path.join(os.getcwd(), location, 'pricelist.csv'), index_col='Unnamed: 0')

    def saveinvoicedb(self, location):
        """Will save the files to a backup folder"""
        self.dataset.to_csv(os.path.join(os.getcwd(), location, 'invoicedb.csv'))
        self.pricelist.to_csv(os.path.join(os.getcwd(), location, 'pricelist.csv'))
        self.message = '==== Invoicedb saved ====\n'
        return self.message

    def getinvoicedb(self, customers=None, fromdate=None, monthcovered=None):
        """Used to exctract a section or totality of the invoicedb"""
        toreturn = self.dataset
        if customers != None:
            mask = toreturn['CustomerID'].isin(customers)
            toreturn = toreturn.loc[mask]
        if fromdate != None or monthcovered != None:
            if fromdate != None and monthcovered != None:
                toreturn = toreturn[toreturn['InvoiceDate'] <= fromdate]
                toreturn = toreturn[toreturn['InvoiceDate'] > fromdate - timedelta(days=30 * monthcovered)]
            else:
                return (False, 'please Indicate fromdate and monthcovered')
        return toreturn
    
if __name__ == "__main__":
    invoicemng = InvoiceMng('')