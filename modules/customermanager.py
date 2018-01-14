#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 14:56:20 2018

@author: cricket
"""

import math
from datetime import datetime
from datetime import timedelta
from scipy import stats
import numpy as np
import pandas as pd


class Customermanager():
    """Will handle and create a customer db file from invoicedb"""
    def __init__(self):
        """Creates the initial variables"""
        self.customerdb = pd.DataFrame()
        self.invoicedb = pd.DataFrame()

    def customerclassstat(self, dataframe):
        """Return a dataframe with only the RFM scores"""
        self.invoicedb = dataframe
        self.light()
        self.updatescores()
        return self.customerdb[['R', 'F', 'M', 'D', 'C']]

    def getdatasetpredict(self, dataframe):
        """Will return the dataset formatted for the prediction system"""
        self.invoicedb = dataframe
        self.light()
        self.complete()
        return self.customerdb

    def getdatasetrmf(self, dataset, weeksextracted=1):
        """Will return the matrice formatted for a randomforrest classification """
        toreturn = pd.DataFrame()
        dates = np.sort(dataset['InvoiceDate'].map(lambda x: x.date()).unique())[::-1]
        #We only keep the last week
        dates = dates[dates >= dates.max()-timedelta(days=7*weeksextracted)]

        counter = 0
        for orderdate in dates:
            counter += 1
            self.invoicedb = dataset[dataset['InvoiceDate'] <= orderdate]

            self.light()
            self.complete()
            self.updatescores()

            #Timestamp on current day set
            self.customerdb['Timestamp'] = self.customerdb['LastInvoice'].apply(lambda x: orderdate)

            #Data assembly
            print('{}-{} - {}'.format(counter, len(dates), orderdate))
            toreturn = pd.concat([toreturn, self.customerdb.reset_index()], axis=0)
            print('dataset size : ', toreturn.shape)
            print('Timestamp -->', datetime.now())

        return toreturn

    def updatescores(self):
        """Will update the scores for customers, the invoicedb must be complete"""
        #rfm score calculation
        scorerange = 4
        divider = 100/scorerange
        self.customerdb['F'] = self.customerdb['Frequency'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['Frequency'], x, kind='mean')/divider))
        self.customerdb['R'] = self.customerdb['Recency'].apply(lambda x: math.ceil(stats.percentileofscore(self.customerdb['Recency'], x, kind='mean')/divider))
        self.customerdb['M'] = self.customerdb['SpentSum'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['SpentSum'], x, kind='mean')/divider))
        self.customerdb['D'] = self.customerdb['SavingsSum'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['SavingsSum'], x, kind='mean')/divider))

        #Calculation of C
        def Cclass(x):
            """Returns the C class parameter"""
            if -x < .02: #2% of cancellation accepted
                return 1
            elif -x < .08: #8% becomes really annoying !
                return 2
            return 3

        self.customerdb['C'] = self.customerdb['RatioCancel'].apply(Cclass)

        self.customerdb['RFMDC'] = self.customerdb['R'] *10000 + self.customerdb['F'] *1000 + self.customerdb['M'] *100 + self.customerdb['D'] *10 + self.customerdb['C'] * 1

    def light(self):
        """Initiate the customerdb aggregation with minimal information
        for RFM score calculation"""

        #Customerdb creation
        date = self.invoicedb['InvoiceDate'].max()
        def my_agg(x):
            '''Minimal features for RFM score calculation'''
            aggcalcul = {
                        'LastInvoice': x['InvoiceDate'].max(),
                        'Recency': (date - x['InvoiceDate'].max()).days,
                        'SpentMin': x['TotalInvoice'].min(),
                        'SpentMax': x['TotalInvoice'].max(),
                        'SpentMean': x['TotalInvoice'].mean(),
                        'SpentSum': x['TotalInvoice'].sum(),
                        'SpentStd': x['TotalInvoice'].std(),
                        'OrderSepMean': x['Ordersep'].mean(),
                        'OrderSepMax' : x['Ordersep'].max(),
                        'OrderSepMin' : x['Ordersep'].min(),
                        'OrderSepStd' : x['Ordersep'].std(),
                        'Frequency' : x['InvoiceDate'].count(),
                        'DiscountMean' : x['Discount'].mean(),
                        'DiscountMax' : x['Discount'].max(),
                        'DiscountMin' : x['Discount'].min(),
                        'DiscountStd' : x['Discount'].std(),
                        'SavingsSum' : x['TotalSavings'].sum(),
                        'SavingsMean' : x['TotalSavings'].mean(),
                        'SavingsMax' : x['TotalSavings'].max(),
                        'SavingsMin' :x['TotalSavings'].min(),
                        'SavingsStd' : x['TotalSavings'].std(),
                        'AmountCancelledSum' : x['AmountCancelled'].sum(),
                        'AmountCancelledMean' : x['AmountCancelled'].mean(),
                        'AmountCancelledMin' : x['AmountCancelled'].min(),
                        'AmountCancelledMax' : x['AmountCancelled'].max(),
                        'AmountCancelledStd' : x['AmountCancelled'].std(),
                        }
            return pd.Series(aggcalcul, index=aggcalcul.keys())
        
        self.customerdb = self.invoicedb.groupby('CustomerID').apply(my_agg).fillna(0)

        #Ratio cancellation calculation
        self.customerdb['RatioCancel'] = self.customerdb['AmountCancelledSum'] / self.customerdb['SpentSum']
        self.customerdb['RatioCancel'] = self.customerdb['RatioCancel'].replace([-np.inf, ], self.customerdb['RatioCancel'].max())

    def complete(self):
        """Will add all the information needed for the RMF matrix"""
        #detail orders for customerdb QUANT UNIT PRICE
        detail = [x for x in self.invoicedb.columns.values if x.startswith('QuantUnitPrice_')]
        detail.append('CustomerID')
        temp = self.invoicedb[detail].groupby('CustomerID').agg([np.sum, np.mean, np.min, np.max, np.std]).fillna(0)
        newnames = ["_".join(x) for x in temp.columns.ravel()]
        temp.columns = newnames
        self.customerdb = self.customerdb.merge(temp, how='inner', left_index=True, right_index=True)

        #detail orders for customerdb QUANT SAVINGS
        detail = [x for x in self.invoicedb.columns.values if x.startswith('QuantUnitSavings_')]
        detail.append('CustomerID')
        temp = self.invoicedb[detail].groupby('CustomerID').agg([np.sum, np.mean, np.min, np.max, np.std]).fillna(0)
        newnames = ["_".join(x) for x in temp.columns.ravel()]
        temp.columns = newnames
        self.customerdb = self.customerdb.merge(temp, how='inner', left_index=True, right_index=True)

        #Time of the day aggregation
        detail = [x for x in self.invoicedb.columns.values if x.startswith('Daytime_Monetary_')]
        detail.append('CustomerID')
        temp = self.invoicedb[detail].groupby('CustomerID').agg([np.sum, np.mean, np.std]).fillna(0)
        newnames = ["_".join(x) for x in temp.columns.ravel()]
        temp.columns = newnames
        self.customerdb = self.customerdb.merge(temp, how='inner', left_index=True, right_index=True)

        #Time of the week aggregation
        detail = [x for x in self.invoicedb.columns.values if x.startswith('Weekday_')]
        detail.append('CustomerID')
        temp = self.invoicedb[detail].groupby('CustomerID').agg([np.sum, np.mean, np.std]).fillna(0)
        newnames = ["_".join(x) for x in temp.columns.ravel()]
        temp.columns = newnames
        self.customerdb = self.customerdb.merge(temp, how='inner', left_index=True, right_index=True)

if __name__ == "__main__":
    customermanager = Customermanager()
    