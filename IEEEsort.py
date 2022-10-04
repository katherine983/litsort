# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 18:28:22 2019

@author: wuest
"""

import sqlite3
import datetime
now=datetime.datetime.now()
import pandas as pd
import numpy as np

import pathlib

def parse(file_to_open, conn, cur, **kwargs):
    fname = file_to_open.name
    repeats = 0
    kwargs['header']=kwargs.get('header', 0)
    kwargs['encoding']=kwargs.get('encoding', 'utf-8')
    db = "IEEE"
    df = pd.read_csv(file_to_open, header=kwargs['header'], encoding= kwargs['encoding'])
    #CLEAN UP CSV COLUMN NAMES
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    
    #key_words = {}
    #gather data into the variables one record at a time
    
    for row in df.itertuples():
        recnum= getattr(row, "Index")
        doi = getattr(row,"DOI")
        pmid = np.nan
        umi = np.nan
        journal = getattr(row, "Publication_Title")
        year = getattr(row, "Publication_Year")
        volume = getattr(row, "Volume")
        issue = getattr(row, "Issue")
        start_page = getattr(row, "Start_Page")       
                
        print(journal)
        print(year, volume, issue)
            
            
        art_title_raw = getattr(row, "Document_Title")
        art_title_nw = art_title_raw.strip()
        art_title_colon = art_title_nw.replace(" :", ":")
        art_title = art_title_colon.title()                #article title in title case and white space stripped from left and right
            
        authors = getattr(row, "Authors")
        print(authors)
        print(type(authors))
        #try:
        auth_names = authors.split(";")
        first_auth_raw = auth_names[0]
        first_auth_split = first_auth_raw.split(" ") 
        lastname_index = len(first_auth_split) - 1
        
        auth_firstname = first_auth_split[:lastname_index]
        auth_firstnames_noperiod = []    
        for n in auth_firstname:
            auth_firstnames_noperiod.append(n.replace(".", "").lower())
                    
        auth_lastname = first_auth_split[lastname_index].lower()
        auth_firstnames = "".join(auth_firstnames_noperiod)
        first_auth = ",".join([auth_lastname, auth_firstnames])
        print(auth_lastname)
        print(auth_firstnames)
                    
        """except:
            first_auth = "noauthor"
            print("Result number", recnum,"has no author." )"""
            
        print(art_title)
        abstract = getattr(row, "Abstract")
            
            
        kws_terms = str(getattr(row, "IEEE_Terms"))
        kws = kws_terms.split(";")
        
        
        print(kws)
            
        url = getattr(row, "PDF_Link")    
                
        #Journals
        cur.execute("""INSERT OR IGNORE INTO Journals (name) VALUES ( ? )""", (journal,))
        cur.execute("""SELECT id FROM Journals WHERE name = ?""", (journal,))
        journal_id = cur.fetchone() [0]
        #First_Authors
        cur.execute("""INSERT OR IGNORE INTO First_Authors (name, lastname, firstname) VALUES ( ?, ?, ?)""", (first_auth, auth_lastname, auth_firstnames))
        cur.execute("""SELECT id FROM First_Authors WHERE name= ?""", (first_auth,))
        if first_auth is None:
            auth_id = None
        else:
            auth_id = cur.fetchone() [0]
        #Databases
        cur.execute("""INSERT OR IGNORE INTO Databases (db) VALUES ( ? )""", (db,))
        cur.execute("""SELECT id FROM Databases WHERE db = ?""", (db,))
        db_id = cur.fetchone() [0]
        #update keywords table
        for kw in kws:
            try:
                cur.execute("""SELECT count FROM Keywords WHERE keyword= ? AND db_id= ?""", (kw, db_id))
                kwcount = cur.fetchone() [0]
                kwcount += 1
                cur.execute("""UPDATE Keywords SET count = ? WHERE keyword = ? AND db_id= ? LIMIT 1""", (kwcount, kw, db_id))
            except:
                cur.execute("""INSERT OR IGNORE INTO Keywords (keyword, db_id, count) 
                  VALUES ( ?, ?, ?)""", (kw, db_id, 0))
        #update Articles
            
        try: 
            cur.execute("""SELECT repeats FROM Articles WHERE (title= ? AND auth_id= ?) OR doi= ? OR pmid= ? OR umi= ?""", (art_title, auth_id, doi, pmid, umi))
            count = cur.fetchone() [0]
            count += 1
            cur.execute("""UPDATE Articles SET repeats = ? WHERE (title= ? AND auth_id= ?) OR doi= ? OR pmid= ? OR umi= ?""", (count, art_title, auth_id, doi, pmid, umi))
            repeats = repeats + 1 
        except:
            cur.execute("""INSERT INTO Articles (title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, repeats, filename) 
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (art_title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, 0, fname))
            
        conn.commit()
    return repeats

if __name__ == '__main__':
    
    while True:
        data_folder_parent = pathlib.Path(r'C:\Users\wuest\Desktop\database_searches\\')
        print("Current directory: ", data_folder_parent)
        data_folder = input("Enter folder location as 'folder\subfolder\': ")
        if len(data_folder)<1 :
            data_folder = 'frailty_and_smarthomes\IEEE_2019-10-07'
        fname = input("Enter file name: ")
        if len(fname)<1 : 
            fname = "IEEE_2019-10-07.csv"
    
        file_to_open = data_folder_parent / data_folder / fname
        try:
            df = pd.read_csv(file_to_open, header=0, encoding= "utf-8")
            break
        except:
            print("Invalid file name please reenter.")
            
    dbfname = 'litdb_default.sqlite'
    conn = sqlite3.connect(dbfname)
    cur = conn.cursor()
    
    
    
    """
    #CREATE DATABASE 
    
    cur.execute('DROP TABLE IF EXISTS First_Authors') #use drop table commands only while troubleshooting initial setup
    cur.execute('DROP TABLE IF EXISTS Articles')
    cur.execute('DROP TABLE IF EXISTS Journals')
    cur.execute('DROP TABLE IF EXISTS Keywords')
    cur.execute('DROP TABLE IF EXISTS Databases')
    
    cur.execute('''CREATE TABLE First_Authors (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            name TEXT UNIQUE,
            lastname TEXT,
            firstname TEXT)''')
    cur.executescript('''CREATE TABLE  Articles (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            title TEXT,
            year YEAR,
            auth_id TEXT,
            doi TEXT UNIQUE,
            pmid INTEGER UNIQUE,
            umi TEXT UNIQUE,
            journal_id INTEGER,
            volume INTEGER,
            issue INTEGER,
            start_page TEXT,
            abstract TEXT,
            url TEXT,
            date_added DEFAULT CURRENT_TIMESTAMP,
            repeats INTEGER,
            filename TEXT);
        CREATE TABLE Journals (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            name TEXT UNIQUE);
        CREATE TABLE Keywords (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            keyword TEXT UNIQUE,
            db_id INTEGER,
            count INTEGER);
        CREATE TABLE Databases (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            db TEXT UNIQUE)''')
    """
    
    # ADD TO DATABASE
    
    repeats = 0
    articles_added = 0
    cur.execute("SELECT count(*) FROM Articles")
    articlecount_starting = cur.fetchone() [0]
    print(articlecount_starting)
    
    
    repeats += parse(file_to_open, conn, cur, header=0)
    
    cur.execute("SELECT count(*) FROM ARTICLES")
    articlecount_ending = cur.fetchone() [0]
    total_changes = conn.total_changes
    conn.close()
    f = open('smarthomelitdblog.txt', "a")
    print("", file=f)
    print("File name added: ", fname, file=f)
    print("Date/time of data add: ", now.isoformat(), file=f)
    print("Article repeats: ", repeats, file=f)
    print("Article count before program ran: ", articlecount_starting, file=f)
    print("Article count after program completed: ", articlecount_ending, file=f)
    print("Total modifications to database: ", total_changes, file=f)
    
    f.close()
    
    
       
        
        
