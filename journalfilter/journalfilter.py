# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 15:19:05 2021

@author: wuest
"""
import pandas as pd
import sqlite3
import pathlib
import re

def _nahrs_title_clean(title):
    title=title.strip()
    title=title.replace('\n','')
    title=title.upper()
    ttext1=re.findall(r'(\D*)[0-9]{4}', title)
    ttext2=[]
    for t in ttext1:
        a=re.sub(r"\D*continues[:]*\s*", "", t, flags=re.IGNORECASE)
        ab=re.sub(r"\D*absorbed[:]*\s*", "", a, flags=re.IGNORECASE)
        b=re.sub(r"\(NONE .*?\)", "", ab, flags=re.IGNORECASE)
        b=re.sub(r" :", ":", b)
        c=re.sub(r"^[(),:;\-. ]+", "", b)
        d=re.sub(r"[,:;\-. ]+$", "", c)
        e=re.sub(r"FEB$", "", d)
        f=re.sub(r"OCT$", "", e)
        g=re.sub(r"NOV$", "", f)
        g=g.replace("  ", " ").strip()
        if len(g)<1:
            continue
        elif re.search("=",g):
            h=re.split("\s*=\s*", g)
            for i in h:
                if re.search("\([A-Z ]+\)*$", i):
                   ha=re.split(" \(", i)
                   j=[l.replace(")", "").strip() for l in ha]
                   ttext2.append(i)
                   ttext2.extend(j) 
                else:
                    ttext2.append(i)
        elif re.search(", THE$", g):
            h=re.sub(", THE$", "", g)
            ttext2.append(g)
            ttext2.append(f'THE {h}')
        elif re.search("\([A-Z ]+\)*$", g):
            h=re.split(" \(", g)
            j=[i.replace(")", "").strip() for i in h]
            ttext2.append(g)
            ttext2.extend(j)
        else:
            ttext2.append(g)
        
        if re.search(r"&", g):
            t1=re.sub("&", "AND", g)
            ttext2.append(t1)
        elif re.search(r"AND", g):
            t1=re.sub("AND", "&", g)
            ttext2.append(t1)
    return ttext2
    
def _title_clean(title):
    title=title.strip()
    title=title.replace('\n','')
    title=title.upper()
    title=title.replace(" :", ":")
    ttext=[title]
    if re.search("=", title):
        t=re.split("\s*=\s*",title)
        t=[i.strip() for i in t]
        ttext.extend(t)
        for i in t:
            if re.search(r"\([\w ]+\)*$", i):
                ha=re.split(" \(", i)
                j=[l.replace(")", "").strip() for l in ha]
                ttext.extend(j)
                for k in j:
                    if re.search(r":", k):
                        t2=re.split(":", k)
                        t3=[i.strip() for i in t2]
                        ttext.extend(t3)
                        for n in t3:
                            if re.search(r"&", n):
                                t4=re.sub("&", "AND", n)
                                ttext.append(t4)
                            elif re.search(r"AND", n):
                                t4=re.sub("AND", "&", n)
                                ttext.append(t4)
    elif re.search(r"\([\w ]+\)*$", title):
        ha=re.split(" \(", title)
        j=[l.replace(")", "").strip() for l in ha]
        ttext.extend(j)
        for k in j:
            if re.search(r":", k):
                t2=re.split(":", k)
                t3=[i.strip() for i in t2]
                ttext.extend(t3)
                for n in t3:
                    if re.search(r"&", n):
                        t4=re.sub("&", "AND", n)
                        ttext.append(t4)
                    elif re.search(r"AND", n):
                        t4=re.sub("AND", "&", n)
                        ttext.append(t4)
    elif re.search(r":", title):
        t2=re.split(":", title)
        t3=[i.strip() for i in t2]
        ttext.extend(t3)
        for n in t3:
            if re.search(r"&", n):
                t4=re.sub("&", "AND", n)
                ttext.append(t4)
            elif re.search(r"AND", n):
                t4=re.sub("AND", "&", n)
                ttext.append(t4)
    elif re.search(r"&", title):
        t4=re.sub("&", "AND", title)
        ttext.append(t4)
    elif re.search(r"AND", title):
        t4=re.sub("AND", "&", title)
        ttext.append(t4)
    return ttext  

def nahrs_read():
    df=pd.read_excel("NAHRS Selected List of Nursing Journals.xlsx", sheet_name="Sheet1", header=0, skiprows=[0,1,2], index_col=None)
    df.columns=df.columns.str.strip()
    df.columns=df.columns.str.replace(' ', '_')
    titles_all=df['Current_Title'].tolist()
    titles=[]
    for title in titles_all:
        titles.extend(_nahrs_title_clean(title))
    return titles

def inane_read():
    df=pd.read_csv("INANE Nursing Journal Directory.csv", header=0, index_col=False)
    df.columns=df.columns.str.replace(' ', '_')
    ttext=df['Journal'].tolist()
    titles=[]
    for t in ttext:
        titles.extend(_title_clean(t))
    return titles

def nlm_read():
    df=pd.read_csv("NLMNursJournalsCatalog.csv", header=0, index_col=False)
    ttext=df['Journal'].tolist()
    titles=[]
    for title in ttext:
        titles.extend(_title_clean(title))
    return titles

def filt(db_path):
    nlm=nlm_read()
    nahrs = nahrs_read()
    inane = inane_read()
    lookup=nlm + nahrs + inane
    lookup=set(lookup)
    adds={'Nordic Journal of Nursing Research & Clinical Studies / Vård i Norden'.upper()}
    lookup.update(adds)
    
    with sqlite3.connect(db_path) as conn:
        cur=conn.cursor()
        cur.execute("SELECT * FROM Journals")
        for row in cur.fetchall():
            j=str(row[1])
            j=j.upper()
            row_id=row[0]
            if j in lookup:
                cur.execute("UPDATE Journals SET nurs=? WHERE id = ?", (1, row_id))
            elif re.search("[\b\s]nurs\w+", j, flags=re.IGNORECASE):
                cur.execute("UPDATE Journals SET nurs=? WHERE id = ?", (1, row_id))
            else:
                continue
        conn.commit()

        edits=conn.total_changes
    return edits

def nurs_auth(filelist, db_path):
    with sqlite3.connect(db_path) as conn:
        cur=conn.cursor()
        for file in filelist:
            cur.execute("UPDATE Articles SET nursauth = ? WHERE filename = ?", (1, file))
        conn.commit()
        cur.execute("SELECT count(*) FROM Articles WHERE nursauth = 1")
        nurseauth=cur.fetchone()[0]
    return nurseauth

    
if __name__ == '__main__':
    db_path=pathlib.Path(r"C:\Users\Wuestney\Documents\GitHub\litsort\complexity_searches_2022\complexity_concept_db.sqlite")
    #nlm=nlm_read()
    #nahrs = nahrs_read()
    #inane = inane_read()
    #pmerg=nlm + nahrs + inane
    #pmerg=set(pmerg)
    print("Number of changes to database when filtering for Nursing Journals.")
    print(filt(db_path))
    #filelist2021=['ProQuestallagingandcomplexityorcomplexandfrailandnurs_2021-06-23.xls', 'pubmed-frailtyMeS-complexorcomplexity-allaging-_2021.06.24.txt', 'MHallagingandcomplexityorcomplexandfrail_nurseauth_2021.06.23.xml', 'complexityorcomplextiorabandallaging_onlynursing_2021.06.24.xls']
    filelist2022 = ['pubmed-frailtyMeS-complexorcomplexity-allaging-_2022.10.03.txt',
                    'ProQuestallagingandcomplexityorcomplexandfrailandnurs_2022-10-03.xls']
    print("Number of articles with a nursing author")
    print(nurs_auth(filelist2022, db_path))

    