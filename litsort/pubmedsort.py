# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 17:31:10 2021

@author: wuest
"""
import sqlite3
import datetime
now=datetime.datetime.now()
import pathlib
import nbib
from dateutil import parser as dateparse
import re

def parse(file_to_open, conn, cur, **kwargs):
    fname = file_to_open.name
    kwargs['encoding']=kwargs.get('encoding', 'utf-8')
    repeats = 0
    fread = file_to_open.read_text(encoding=kwargs['encoding'])
    txt = nbib.read(fread)
    db = 'PUBMED'
    recnum=0
    for ref in txt:
        recnum= recnum + 1
        print("Rec number: ", recnum)
        doi = ref.get('doi', None)     #doi
        print(doi)
        pmid = ref['pubmed_id']       #pmid
        print(pmid)
        umi =  None           #umi
        journaltext = ref['journal']
        journalnw = journaltext.strip()
        journal_colon = journalnw.replace(" :", ":")
        journal_np = journal_colon.replace("...", " ")
        journal_semic = journal_np.replace(" ;", ";")
        journal = journal_semic.title()
        pub_date_str = ref['publication_date']
        try:
            pub_date = dateparse.parse(pub_date_str)
            year = int(pub_date.year)          #year
        except KeyError as KE:
            print(KE, "\n recnum {} does not have publication date".format(recnum))
            continue
        except ValueError as VE:
            print(VE, "unable to parse publication date into datetime object")
            pub_date = re.match('(\d\d\d\d) ', pub_date_str)
            if pub_date:
                year = int(pub_date[0])
            else: 
                year = None
        volume = ref.get('journal_volume', None)            #volume
        issue = ref.get('journal_issue', None)           #issue
        pagerange = ref.get('pages', None)  
        try:
            pages = pagerange.split('-')
            start_page = str(pages[0])                                       #first page number of article
            print(start_page)
        except:
            start_page = str(pagerange)
        print(journal)
        print(year, volume, issue)
        art_title_raw = ref['title']
        art_title_nw = art_title_raw.strip()
        art_title_colon = art_title_nw.replace(" :", ":")
        art_title = art_title_colon.title()                #article title in title case and white space stripped from left and right
        authors= ref.get('authors', None)
        if authors:
            first_author = authors[0]
            auth_lastname = first_author.get('last_name', None)
            print(auth_lastname)
            auth_firstnames = first_author.get('first_name', None)
            print(auth_firstnames)
            try:
                auth_firstname = auth_firstnames.replace(' ', '.')
                authornames = [auth_lastname, auth_firstname]
                authnames = ",".join(authornames)
                first_auth = authnames.lower()
                print(first_auth)
                
            except:
                first_auth = "noauthor"
                print("Result number", recnum,"has no author." )
        else:
            auth_group = ref.get('corporate_author', None)
            first_auth = auth_group
            
        
        print(art_title)
        abstract = ref.get('abstract', None)
        
        
        kws = []
        descriptors = ref.get('descriptors', None)
        if descriptors:
            for d in descriptors:
                if d['major'] == True:
                    kws.append(d['descriptor'])
        
        
        try:
            url = "https://www.ncbi.nlm.nih.gov/pubmed/" + pmid
        except:
            url = None
        
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
        for key_word in kws:
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
            cur.execute("""INSERT INTO Articles (title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, repeats, filename) 
                    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (art_title, year, auth_id, doi, pmid, umi, journal_id, volume, issue, start_page, abstract, url, 0, fname))
        
        conn.commit()
    return repeats

if __name__ == '__main__':
    while True:
        data_folder_parent = pathlib.Path(r'C:\Users\wuest\Desktop\complexity_searches\\')
        data_folder = input("Enter folder location as 'folder\subfolder': ")
        if len(data_folder)<1 :
            data_folder = 'PUBMED'
        fname = input("Enter file name: ")
        if len(fname)<1 : 
            fname = "pubmed-elderlyfra-set.txt"
    
        #file_to_open = "{}{}\{}".format(data_folder_parent, data_folder, fname)
        file_to_open = data_folder_parent / data_folder / fname
        try:
            fread = file_to_open.read_text(encoding='utf8')
            break
        except:
            print("Invalid file name please reenter.")
            
    
    conn = sqlite3.connect('litdb_default.sqlite')
    cur = conn.cursor()
    
    
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
                title TEXT UNIQUE,
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
    
    db='PUBMED'
    
    repeats = 0
    articles_added = 0
    cur.execute("SELECT count(*) FROM Articles")
    articlecount_starting = cur.fetchone() [0]
    print(articlecount_starting)
    
    repeats += parse(file_to_open, conn, cur)
    
    cur.execute("SELECT count(*) FROM ARTICLES")
    articlecount_ending = cur.fetchone() [0]
    total_changes = conn.total_changes
    conn.close()
    f = open('complexitylitdblog.txt', "a")
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
    
    
    f.close()
