# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 18:28:22 2019

@author: wuest
"""

import sqlite3
import xml.etree.ElementTree as ET
import datetime
now=datetime.datetime.now()
import pathlib
import re

def parse(file_to_open, conn, cur, **kwargs):
    fname= file_to_open.name
    kwargs['encoding']=kwargs.get('encoding', 'utf-8')
    kwargs['db']=kwargs.get('db', 'CINAHL')
    repeats = 0
    with open(file_to_open, encoding=kwargs['encoding']) as fhand:
        
        txt = ET.parse(fhand)
        
        #print(len(rec))
        
        
        #key_words = {}
        #gather data into the variables one record at a time
        db = kwargs['db']
        rec = txt.findall("rec")
        for r in rec:   
            print("Rec number: ", r.get('resultID'))
            #database = r.find(".//header")
            #db = database.get('longDbName')
            print(db)
            info = r.find('.//controlInfo') #list of xml elements under controlInfo parent. each element may have children too.
            doi = r.findtext(".//artinfo/ui[@type='doi']")          #doi
            print(doi)
            pmid = r.findtext(".//artinfo/ui[@type='pmid']")        #pmid
            print(pmid)
            umi = r.findtext(".//artinfo/ui[@type='umi']")
            print(umi)
            journal = r.findtext(".//jtl") #find all children with tag 'jtl' and return text to journal
            y = r.find(".//pubinfo/dt")
            year = y.get("year")                           #year
            volume = r.findtext(".//vid")                  #volume
            issue = r.findtext(".//iid")                   #issue
            start_page = r.findtext(".//ppf")              #first page number of article
            print(journal)
            print(year, volume, issue)
            
            art_title_raw = r.findtext(".//atl")
            art_title_nw = art_title_raw.strip()
            art_title_colon = art_title_nw.replace(" :", ":")
            art_title = art_title_colon.title()                #article title in title case and white space stripped from left and right
            
            author = r.findtext(".//aug/au")            #article first author
            if author == None:
                first_auth = "noauthor"
                print("Entry has no author.", art_title, doi, pmid)
            else:
                author_nw = author.replace(',', '')
                author_np = author_nw.replace('.','')
                author_lower = author_np.lower()
                authnames = author_lower.split()
                auth_lastname = authnames[0]
                auth_firstnames = '.'.join(authnames[1:])
                authorname = [auth_lastname, auth_firstnames]
                first_auth = ",".join(authorname)
            print(first_auth)
            print(art_title)
            authaffil = r.findall(".//aug/affil")
            nursauth=False
            for a in authaffil:
                if re.search("nurs", a.text, re.IGNORECASE):
                    nursauth=True
            ab = r.findtext(".//ab")
            if ab == None:
                abstract = ab
            else:
                abstract = ab.strip()                          #abstract with white space stripped from left and right
            
            kws = r.findall(".//sug/*subj")
            
            
                
            url = r.findtext(".//displayInfo/pLink/url")
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
                key_word = kw.text
                
                try:
                    cur.execute("""SELECT count FROM Keywords WHERE keyword= ? AND db_id= ?""", (key_word, db_id))
                    kwcount = cur.fetchone() [0]
                    kwcount += 1
                    cur.execute("""UPDATE Keywords SET count = ? WHERE keyword = ? AND db_id= ? LIMIT 1""", (kwcount, key_word, db_id))
                except:
                    cur.execute("""INSERT OR IGNORE INTO Keywords (keyword, db_id, count) 
                  VALUES ( ?, ?, ?)""", (key_word, db_id, 0))
            #update Articles
            
            try: 
                cur.execute("""SELECT repeats FROM Articles WHERE (title= ? AND auth_id= ?) OR doi= ? OR pmid= ? OR umi= ?""", (art_title, auth_id, doi, pmid, umi))
                count = cur.fetchone() [0]
                count += 1
                cur.execute("""UPDATE Articles SET repeats = ? WHERE (title= ? AND auth_id= ?) OR doi= ? OR pmid= ? OR umi= ?""", (count, art_title, auth_id, doi, pmid, umi))
                repeats = repeats + 1 
            except:
                cur.execute("""INSERT INTO Articles (title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, repeats, filename, nursauth) 
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (art_title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, 0, fname, int(nursauth)))
            conn.commit()
    return repeats
    """
    elif db == 'PUBMED':
        rec = txt.findall("PubmedArticle")
        recnum= 0
        for r in rec:    
            recnum= recnum + 1
            print("Rec number: ", recnum)
            print(db)
            #info = r.find('.//controlInfo') #list of xml elements under controlInfo parent. each element may have children too.
            doi = r.findtext(".//ArticleIdList/*[@IdType='doi']")          #doi
            print(doi)
            pmid = r.findtext(".//ArticleIdList/*[@IdType='pubmed']")        #pmid
            print(pmid)
            umi = r.findtext(".//ArticleIdList/*[@IdType='umi']")           #umi
            print(umi)
            journaltext = r.findtext(".//Journal/Title") #find all children with tag 'Title' and parent 'Journal' and return text to journal
            journalnw = journaltext.strip()
            journal_colon = journalnw.replace(" :", ":")
            journal_np = journal_colon.replace("...", " ")
            journal_semic = journal_np.replace(" ;", ";")
            journal = journal_semic.title()
            year = r.findtext(".//JournalIssue/PubDate/Year")                           #year
            volume = r.findtext(".//JournalIssue/Volume")                  #volume
            issue = r.findtext(".//JournalIssue/Issue")                   #issue
            pagerange = r.findtext(".//MedlinePgn")         
            try:
                pages = pagerange.split('-')
                start_page = str(pages[0])                                       #first page number of article
                print(start_page)
            except:
                start_page = str(pagerange)
                
            
            print(journal)
            print(year, volume, issue)
            
            
            art_title_raw = r.findtext(".//ArticleTitle")
            art_title_nw = art_title_raw.strip()
            art_title_colon = art_title_nw.replace(" :", ":")
            art_title = art_title_colon.title()                #article title in title case and white space stripped from left and right
            
           
            auth_lastname = r.findtext(".//LastName")
            print(auth_lastname)
            auth_firstnames = r.findtext(".//ForeName")
            print(auth_firstnames)
            auth_group = r.findtext(".//CollectiveName")
            if auth_lastname and auth_firstnames is None:
                first_auth = auth_group
            else:
                try:
                    auth_firstname = auth_firstnames.replace(' ', '.')
                    authornames = [auth_lastname, auth_firstname]
                    authnames = ",".join(authornames)
                    first_auth = authnames.lower()
                    print(first_auth)
                    
                except:
                    first_auth = "noauthor"
                    print("Result number", recnum,"has no author." )
            
            print(art_title)
            ab = r.findall(".//AbstractText")
            if ab is None:
                abstract = ab
                
            else:
                ablist = []
                try:
                    for a in ab:
                        abits = ' '.join(a.itertext())
                        ablist.append(abits)
                    abstract = ' '.join(ablist)
                except:
                    for a in ab:
                        ablist.append(a.text)
                    abstract = " ".join(ablist)
            
            kws = r.findall(".//MeshHeading/DescriptorName/*[@MajorTopicYN='Y']")
            
            
            try:
                url = "https://www.ncbi.nlm.nih.gov/pubmed/" + pmid
            except:
                url = None
                
            #Journals
            cur.execute('''INSERT OR IGNORE INTO Journals (name) VALUES ( ? )''', (journal,))
            cur.execute('''SELECT id FROM Journals WHERE name = ?''', (journal,))
            journal_id = cur.fetchone() [0]
            #First_Authors
            cur.execute('''INSERT OR IGNORE INTO First_Authors (name, lastname, firstname) VALUES ( ?, ?, ?)''', (first_auth, auth_lastname, auth_firstnames))
            cur.execute('''SELECT id FROM First_Authors WHERE name= ?''', (first_auth,))
            if first_auth is None:
                auth_id = None
            else:
                auth_id = cur.fetchone() [0]
            #Databases
            cur.execute('''INSERT OR IGNORE INTO Databases (db) VALUES ( ? )''', (db,))
            cur.execute('''SELECT id FROM Databases WHERE db = ?''', (db,))
            db_id = cur.fetchone() [0]
            #update keywords table
            for kw in kws:
                key_word = kw.text
                
                try:
                    cur.execute('''SELECT count FROM Keywords WHERE keyword= ? AND db_id= ?''', (key_word, db_id))
                    kwcount = cur.fetchone() [0]
                    cur.execute('''UPDATE Keywords SET count = ? WHERE keyword = ? AND db_id= ? LIMIT 1''', (kwcount + 1, key_word, db_id))
                except:
                    cur.execute('''INSERT OR IGNORE INTO Keywords (keyword, db_id, count) 
                  VALUES ( ?, ?, ?)''', (key_word, db_id, 0))
            #update Articles
            
            try: 
                cur.execute('''SELECT repeats FROM Articles WHERE (title= ? AND auth_id= ?) OR doi= ? OR pmid= ? OR umi= ?''', (art_title, auth_id, doi, pmid, umi))
                count = cur.fetchone() [0]
                cur.execute('''UPDATE Articles SET repeats = ? WHERE (title= ? AND auth_id= ?) OR doi= ? OR pmid= ? OR umi= ?''', (count + 1, art_title, auth_id, doi, pmid, umi))
                repeats = repeats + 1 
            except:
                cur.execute('''INSERT INTO Articles (title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, repeats, filename) 
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (art_title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, 0, fname))
            
            conn.commit()
    else: 
        print("Database unrecognized")
        """
if __name__ == '__main__':
    
    while True:
        data_folder_parent = pathlib.Path(r'C:\Users\wuest\Desktop\complexity_searches\\')
        data_folder = input("Enter folder location as 'folder\subfolder\': ")
        if len(data_folder)<1 :
            data_folder = 'HSN'
        fname = input("Enter file name: ")
        if len(fname)<1 : 
            fname = "HSNallagingandolderpatientsandcomplexityorcomplexandfrail_2021.06.24.xml"
    
        file_to_open = data_folder_parent / data_folder / fname
        try:
            file_to_open.resolve(strict=True)
            
            break
        except:
            print("Invalid file name please reenter.")
            
    with sqlite3.connect('litdb_default.sqlite') as conn:
        cur = conn.cursor()
        
        db = input("Enter database in all caps: ")
        
        first_entry= input("Create new or overwrite existing complexity_concept_db.sqlite database? Y/N")
        
        if first_entry.upper() == 'Y':
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
                    filename TEXT,
                    nursauth INT);
                CREATE TABLE Journals (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                    name TEXT UNIQUE);
                CREATE TABLE Keywords (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                    keyword TEXT UNIQUE,
                    db_id INTEGER,
                    count INTEGER);
                CREATE TABLE Databases (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                    db TEXT UNIQUE)''')
        
        repeats = 0
        articles_added = 0
        cur.execute("SELECT count(*) FROM Articles")
        articlecount_starting = cur.fetchone() [0]
        print(articlecount_starting)
        
        
        repeats += parse(file_to_open, conn, cur, db='HSN')
        
        cur.execute("SELECT count(*) FROM ARTICLES")
        articlecount_ending = cur.fetchone() [0]
        total_changes = conn.total_changes
        with open('litdb_defaultlog.txt', "a") as f:
            print("", file=f)
            if first_entry.upper() == 'Y':
                print("Database reset.", file=f)
                print("", file=f)
            print("File name added: ", fname, file=f)
            print("Date/time of data add: ", now.isoformat(), file=f)
            print("Article repeats: ", repeats, file=f)
            print("Article count before program ran: ", articlecount_starting, file=f)
            print("Article count after program completed: ", articlecount_ending, file=f)
            print("Total modifications to database: ", total_changes, file=f)
            

    
    
       
        
        
