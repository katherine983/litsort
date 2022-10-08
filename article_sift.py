# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 15:53:58 2021

@author: wuest
"""
import sqlite3
import re
from contextlib import closing
import pandas as pd

#pattern=re.compile('(complex\S*)\s')
entries=[]
errorlog=[]
df=pd.read_excel('complexity_searches_2022/article_export_2022.xlsx', sheet_name='article_export', header=0)
abstracts=df[['id','title','abstract']].to_dict('records')
for row in abstracts:
    abstract=row['abstract']
    title=row['title']
    try:
        mentions = re.findall(r"(complex\w*)[\s:;',\".?!]*", row['abstract'], flags=re.IGNORECASE)
        title_use= re.findall(r"(complex\w*)[\s:;,'\".?!]*", row['title'], flags=re.IGNORECASE)
        complexity_mentions = re.findall(r"(complexity)[\s:;'\",.?!]*", row['abstract'], flags=re.IGNORECASE)
        complexity_title_use= re.findall(r"(complexity)[\s:;,'\".?!]*", row['title'], flags=re.IGNORECASE)
        entries.append({'id':row['id'], 'title_use':len(title_use), 'abstract_mentions_count':len(mentions), 'complexity_mentions':len(complexity_mentions), 'complexity_title_use':len(complexity_title_use)})
    except TypeError as TE:
        errorlog.append((row['id'], TE))
        
df1= pd.DataFrame(entries)
df_update=pd.merge(df, df1, how='outer', on='id')
df_update.sort_values(by=['abstract_mentions_count'], ascending=False, inplace=True)
df_complexity=df_update.query('complexity_mentions > 0 | complexity_title_use>0')
#df_update.to_csv('article_export.csv', sep='\t')
df_complexity.to_csv('article_export_complexity_2022.csv', sep='\t')
print("")
