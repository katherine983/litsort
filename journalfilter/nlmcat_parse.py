# -*- coding: utf-8 -*-
"""
Created on Fri Jun 25 14:12:36 2021

@author: wuest
"""
import sqlite3
import xml.etree.ElementTree as ET
import datetime
import pathlib
import csv
from functools import reduce
import pandas as pd

def nlm_parse():
    with open("nlmcatalog_result_Nsubset.xml", encoding='utf-8') as nlm:
        with open("NLMNursJournalsCatalog.csv", 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['NCBI_ID', 'Journal', 'Publishers', 'Country_of_Publication', 'Language', 'Description', 'eISSN', 'ISSN', 'Year_1st_Published', 'Journal_URL']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            #parse journal data
            txt=ET.parse(nlm)
            rec = txt.findall("NCBICatalogRecord")
            for record in rec:
                row = {}
                row['NCBI_ID'] = record.get('ID')
                print(row['NCBI_ID'])
                r = record.find(".//JrXml")
                row['Journal'] = r.findtext(".//Title")
                row['Publishers'] = r.findtext(".//Publisher")
                row['Country_of_Publication'] = r.findtext(".//Country")
                row['Language'] = r.findtext(".//Language[@LangType='Primary']")
                descrip = r.iter(tag="GeneralNote")
                row['Description'] = reduce(lambda a, b: f'{a} {b}', [d.text for d in descrip], '')
                row['eISSN'] = r.findtext(".//ISSN[@IssnType='Electronic']")
                row['ISSN'] = r.findtext(".//ISSN[@IssnType='Print']")
                row['Year_1st_Published'] = r.findtext(".//PublicationFirstYear")
                row['Journal_URL'] = r.findtext(".//IndexingSelectedURL")
                writer.writerow(row)

if __name__ == "__main__":
    nlm_parse()