#!/usr/bin/env python
import snowflake.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def getResultSQL(sqlcommand :  str):
    conn = snowflake.connector.connect(
        user='mcsandoval',
        password="Mcps2147808400-",
        account='ve36598.us-east-2.aws'
    )
    try:
        cur = conn.cursor().execute(sqlcommand)
        while(True):
            ret = cur.fetchmany(1)
            if(len(ret) == 0):
                break
            print(ret[0])
    finally:
        conn.close()
        print("Server Closed")

def getCompanyName(companySymbol : str,conn):
    try:
        sqlcommand = "select companyname from (US_STOCKS_DAILY.PUBLIC.COMPANY_PROFILE) where symbol like '"+ companySymbol +"';"
        cur = conn.cursor().execute(sqlcommand)
        ret = cur.fetchmany(1)
        print("%s" % ret[0])
    finally:
        return ret[0]

def getCompanyDailyData(companySymbol : str,conn):
    try:
        sqlCommand = "select date, open from (US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY) where symbol like '"+ companySymbol +"';"
        companyData = {}
        result = conn.cursor().execute(sqlCommand).fetchall()
        for ret in result:
            companyData[ret[0]] = ret[1]
            print("date: %s Open: %s" % (ret[0],ret[1]))
    finally:
        return companyData


def linearModel(companyData : dict):
    pass


def getHigh(comapanySymbol : str,conn):
    try:
        sqlCommand = "select high from(US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY) where symbol like '" + comapanySymbol + "';"
        companyHigh = []
        result = conn.cursor().execute(sqlCommand).fetchall()
        for ret in result:
            companyHigh.append(ret[0])
            print(ret[0])
    finally:
        companyHigh

def getLow(companySymbol : str,conn):
    try:
        sqlCommand = "select low from(US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY) where symbol like '" + companySymbol + "';"
        companyLow = []
        result = conn.cursor().execute(sqlCommand).fetchall()
        for ret in result:
            companyLow.append(ret[0])
            print(ret[0])
    finally:
        companyLow

def main():
    # sqlCommandECompany = "select distinct symbol from (US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY);"
    
    # getResultSQL(sqlCommand)
    conn = snowflake.connector.connect(
        user='mcsandoval',
        password="Mcps2147808400-",
        account='ve36598.us-east-2.aws'
    )
    try:
        # run all the functions here
        getCompanyName("GNC",conn)
        getCompanyDailyData("GNC",conn)

    finally:
        conn.close()
        print("Database closed")



main()