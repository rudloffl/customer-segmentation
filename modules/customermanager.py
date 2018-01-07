#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  5 14:56:20 2018

@author: cricket
"""

from datetime import datetime
from scipy import stats
import math
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

    def getdatasetrmf(self, dataframe):
        """Will return the matrice formatted for a randomforrest classification """
        self.invoicedb = dataframe

    def updatescores(self):
        """Will update the scores for customers, the invoicedb must be complete"""
        #rfm score calculation
        scorerange = 4
        divider = 100/scorerange
        self.customerdb['F'] = self.customerdb['Frequency'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['Frequency'], x, kind='mean')/divider))
        self.customerdb['R'] = self.customerdb['Recency'].apply(lambda x: math.ceil(stats.percentileofscore(self.customerdb['Recency'], x, kind='mean')/divider))
        self.customerdb['M'] = self.customerdb['SpentSum'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['SpentSum'], x, kind='mean')/divider))
        self.customerdb['D'] = self.customerdb['SavingsSum'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['SavingsSum'], x, kind='mean')/divider))

        scorerange = 3
        divider = 100/scorerange
        self.customerdb['C'] = self.customerdb['AmountCancelledSum'].apply(lambda x: scorerange + 1 - math.ceil(stats.percentileofscore(self.customerdb['AmountCancelledSum'], x, kind='mean')/divider))
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
                'SpentSum': x['TotalInvoice'].sum(),
                'SavingsSum' : x['TotalSavings'].sum(),
                'Frequency' : x['InvoiceDate'].count(),
                'AmountCancelledSum' : x['AmountCancelled'].sum()
                }
            return pd.Series(aggcalcul, index=aggcalcul.keys())
        self.customerdb = self.invoicedb.groupby('CustomerID').apply(my_agg).fillna(0)

    def complete(self):
        """Will add all the information needed for the RMF matrix"""
        pass

if __name__ == "__main__":
    customermng = Customermanager()
    