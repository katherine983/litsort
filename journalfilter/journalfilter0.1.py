# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 15:19:05 2021

@author: wuest

this version is deprecated.

uses pandas dataframes to merge all journal information.
"""
import pandas as pd
import sqlite3
import pathlib



def nahrs_read():
    #nahrs contains Journal Title, Journal URL, Publishers, ISSN, eISSN, Country, Year 1st Published fields
    df=pd.read_excel("NAHRS Selected List of Nursing Journals.xlsx", sheet_name="Sheet1", header=0, skiprows=[0,1,2], index_col=None)
    df.columns=df.columns.str.strip()
    df.columns=df.columns.str.replace(' ', '_')
    jtitles= df.pop("Current_Title")
    jtitle=jtitles.str.extract(r'(?P<Journal>[a-zA-Z ]*) [0-9]{4}\-')
    df['ISSN'] = df['ISSN'].str.strip()
    df['ISSN_Electronic']=df['ISSN_Electronic'].str.lstrip()
    df['Journal']=jtitle
    df.rename(columns={"ISSN_Electronic":"eISSN", "Current_Publisher":"Publishers", "Year1st_Published":"Year_1st_Published"}, inplace=True)
    print(df.columns)
    return df

def inane_read():
    #inane contains Journal Title, Journal URL, Publishers fields
    df=pd.read_csv("INANE Nursing Journal Directory.csv", header=0, index_col=False)
    df.columns=df.columns.str.replace(' ', '_')
    return df

def nlm_read():
    #nlm contains Journal Title, Journal URL, Publishers, ISSN, eISSN, Country, Year 1st Published fields
    df=pd.read_csv("NLMNursJournalsCatalog.csv", header=0, index_col=False)
    return df
def filt(db_path):
    nlm=nlm_read()
    nahrs = nahrs_read()
    inane = inane_read()
    pmerg=pd.merge(nlm, nahrs, how="outer", on=['Journal','ISSN'])
    lookup=pd.merge(pmerg, inane, how="outer", on=['Journal'])
    
    with sqlite3.connect(db_path) as conn:
        cur=conn.cursor()
        for row in cur.execute("SELECT * FROM Journals"):
            j=str(row['name'])
            id=row[0]
            present=lookup['Journal'].str.contains(j, regex=False, case=False)
            if sum(present)>0:
                cur.execute("UPDATE Journals SET nurs=? WHERE id")
        
if __name__ == '__main__':
    fopen=pathlib.Path(r"C:\Users\wuest\Desktop\complexity_searches\complexity_concept_db.sqlite")
    nlm=nlm_read()
    nahrs = nahrs_read()
    inane = inane_read()
    pmerg=pd.merge(nlm, nahrs, how="outer", on=['Journal','ISSN'])
    lookup=pd.merge(pmerg, inane, how="outer", on=['Journal'])