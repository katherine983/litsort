# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 11:40:04 2021

@author: wuest
"""
import sqlite3
import datetime
now=datetime.datetime.now()
import pathlib
import json
import pubmedsort
import ebscosort
import Psycsort
import IEEEsort
import wossort

funcs = {'PUBMED': lambda: pubmedsort.parse(file_to_open, conn, cur), 'CINAHL': lambda: ebscosort.parse(file_to_open, conn, cur, db='CINAHL'), 'PSYCINFO':lambda: Psycsort.parse(file_to_open, conn, cur), 'IEEE': lambda: IEEEsort.parse(file_to_open, conn, cur), 'WOS': lambda: wossort.parse(file_to_open, conn, cur), 'HSN':lambda: ebscosort.parse(file_to_open, conn, cur, db='HSN')}
iterdir=False

newprojdir=input("New project directory? Y/N ")
if newprojdir.upper() == 'Y':
    while True:
        parent_dir_txt = input(r"""Enter the name of the folder path containing the new project directory: (e.g. C:\Users\wuest\Desktop\litsort)""")
        try:
            data_folder_parent= pathlib.Path(parent_dir_txt)
            data_folder_parent.resolve(strict=True)
            break
        except FileNotFoundError:
            print("file path not found. please enter valid path")
            retryparentdir = input("Try again? Y/N (Enter 'N' to exit program): ")
            if retryparentdir.upper() == 'Y':
                continue
            else:
                quit()

    while True:
        data_folder = input("Enter subfolder location of search results to be added to db: ")
        try:
            file_root = data_folder_parent / data_folder
            file_root.resolve(strict=True)
            break
        except FileNotFoundError:
            print("file path not found. please enter valid data folder location.")
            retrysubdir = input("Try again? Y/N (Enter 'N' to exit program): ")
            if retrysubdir.upper() == 'Y':
                continue
            else:
                quit()
    while True:
        fname = input("Enter name of file (including extension) to be added to db. If none, press Enter: ")
        if len(fname)<1:
            fname = None
            print("No filename given. Will add all files in given directory to the db.")
            iterdir=True
            break
        else:
            file_to_open = file_root / fname
            try:
                file_to_open.resolve(strict=True)
                break
            except FileNotFoundError:
                print("file path not found. please enter valid filename.")
                retryfilename = input("Try again? Y/N (Enter 'N' to exit program): ")
                if retryfilename.upper() == 'Y':
                    continue
                else:
                    quit()
    while True:            
        db_name = input('Enter name of database file to use. (Note make sure db file is located under the project folder. ')
        new_db = input('New database? Y/N')
        try:
            db_path = data_folder_parent / db_name
            if new_db.upper() == 'Y':
                db_path.resolve(strict=False)
            else:
                db_path.resolve(strict=True)
            break
        except FileNotFoundError:
            print("file path not found. please enter valid db filename, make sure to inclue file extension.")
            retrydb = input("Try again? Y/N (Enter 'N' to exit program): ")
            if retrydb.upper() == 'Y':
                continue
            else:
                quit()
    settings_json_dict = {'recent_path_parts' : {'project_folder': str(data_folder_parent), 'data_subfolder' : str(data_folder), 'filename': fname}, 'db_path': str(db_path)}
    if fname:
        settings_json_dict['recent_path'] = [str(file_to_open), 'is_file']
    else:
        settings_json_dict['recent_path'] = [str(file_root), 'is_directory']
    
    with open('recent_dir.txt', 'w') as recent_dir:
        json.dump(settings_json_dict, recent_dir)

else:
    with open('recent_dir.txt', "r+") as recent_dir:
        dir_settings = json.load(recent_dir)
        recent_dir.seek(0)
        print("Recent file path:\n ", dir_settings['recent_path_parts'])
        print("Recent db:\n ", dir_settings['db_path'])
        #recent_path = pathlib.Path(dir_settings['recent_path'])
        q = input('Continue? Y/N ')
        if q.upper() == 'Y':    
            data_folder_parent = pathlib.Path(dir_settings['recent_path_parts']['project_folder'])
            data_folder = dir_settings['recent_path_parts']['data_subfolder']
            file_root = data_folder_parent / data_folder
            fname = dir_settings['recent_path_parts']['filename']
            db_path = pathlib.Path(dir_settings['db_path'])
            print('Current data subfolder: {}'.format(str(file_root)))
            new_subfolder = input('Change data subfolder? (To parse all files in a subfolder, enter Y and reenter folder name.) Y/N ')
            if new_subfolder.upper() == 'Y':
                while True:
                    data_folder = input('Enter name of data subfolder containing data to add to db: ')
                    try:
                        file_root=data_folder_parent / data_folder
                        file_root.resolve(strict=True)
                        dir_settings['recent_path_parts']['data_subfolder']=data_folder
                        break
                    except FileNotFoundError:
                        print("file path not found. please enter valid data folder location.")
                        retrysubdir = input("Try again? Y/N (Enter 'N' to exit program): ")
                        if retrysubdir.upper() == 'Y':
                            continue
                        else:
                            quit()
                
                while True:
                    get_new_fname = input("Enter name of file (including extension) to be added to db. If none, press Enter: ")
                    if len(get_new_fname)<1:
                        fname = None
                        dir_settings['recent_path_parts']['filename']=fname
                        dir_settings['recent_path'] = [str(file_root), 'is_directory']
                        print("No filename given. Will add all files in given directory to the db.")
                        iterdir=True
                        json.dump(dir_settings, recent_dir)
                        recent_dir.truncate()
                        break
                    else:
                        fname=get_new_fname
                        file_to_open = file_root / fname
                        try:
                            file_to_open.resolve(strict=True)
                            dir_settings['recent_path_parts']['filename']=fname
                            dir_settings['recent_path'] = [str(file_to_open), 'is_file']
                            json.dump(dir_settings, recent_dir)
                            recent_dir.truncate()
                            break
                        except FileNotFoundError:
                            print("file path not found. please enter valid filename.")
                            retryfilename = input("Try again? Y/N (Enter 'N' to exit program): ")
                            if retryfilename.upper() == 'Y':
                                continue
                            else:
                                quit()
                #if len(get_new_fname)>0:
                 #   fname = get_new_fname
                  #  dir_settings['recent_path_parts']['filename']=fname
                #else:
                #    fname=None
                #if not fname:
                #    print("No filename given. Will add all files in given directory to the db.")
                #    iterdir = True
                #    dir_settings['recent_path'] = [str(file_root), 'is_directory']
                #elif fname:
                #    file_to_open = file_root / fname
                #    dir_settings['recent_path'] = [str(file_to_open), 'is_file']
                
            else:
                while True:
                    get_new_fname = input('Enter name of file to add to db. Or press Enter to use the filename from recent settings. ')
                    if len(get_new_fname)>0:
                        fname = get_new_fname
                        file_to_open = file_root / fname
                        try:
                            file_to_open.resolve(strict=True)
                            dir_settings['recent_path_parts']['filename']=fname
                            dir_settings['recent_path'] = [str(file_to_open), 'is_file']
                            json.dump(dir_settings, recent_dir)
                            recent_dir.truncate()
                            break
                        except FileNotFoundError:
                            print("file path not found. please enter valid filename.")
                            retryfilename = input("Try again? Y/N (Enter 'N' to exit program): ")
                            if retryfilename.upper() == 'Y':
                                continue
                            else:
                                quit()
                        break
                    if not fname:
                        print("No filename given. Will add all files in given directory to the db.")
                        iterdir = True
                        break
                    elif fname:
                        file_to_open = file_root / fname
                        try:
                            file_to_open.resolve(strict=True)
                            break
                        except FileNotFoundError:
                            print("file path not found. please enter valid filename.")
                            retryfilename = input("Try again? Y/N (Enter 'N' to exit program): ")
                            if retryfilename.upper() == 'Y':
                                continue
                            else:
                                quit()
            
                    """
                    if len(get_new_fname)>0:
                        fname = get_new_fname
                        dir_settings['recent_path_parts']['filename']=fname
                    else:
                        fname = None
                        dir_settings['recent_path_parts']['filename']=fnamedir_settings['recent_path_parts']['filename']=fname
                    if not fname:
                        print("No filename given. Will add all files in given directory to the db.")
                        iterdir = True
                    elif fname:
                        file_to_open = file_root / fname
                        dir_settings['recent_path'] = [str(file_to_open), 'is_file']
                        """
        else:
            quit()                  

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    
    first_entry= input("Create new/overwrite existing complexity_concept_db.sqlite database? Y/N")
    
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
                nursauth INTEGER);
            CREATE TABLE Journals (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                name TEXT UNIQUE,
                nurs INTEGER);
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
    
    if iterdir:
        for file_to_open in file_root.iterdir():
            if file_to_open.is_dir():
                continue
            else:
                repeats += funcs[data_folder]()
    else:
        repeats = funcs[data_folder]()
    
    cur.execute("SELECT count(*) FROM ARTICLES")
    articlecount_ending = cur.fetchone() [0]
    total_changes = conn.total_changes
    
    log_file = db_path.stem + '_log.txt'
    log_file_path = data_folder_parent / log_file
    f = open(log_file_path, "a")
    print("", file=f)
    if first_entry.upper() == 'Y':
        print("Database reset.", file=f)
        print("", file=f)
    print(F"File name added: {data_folder} / {fname}", file=f)
    print("Date/time of data add: ", now.isoformat(), file=f)
    print("Article repeats: ", repeats, file=f)
    print("Article count before program ran: ", articlecount_starting, file=f)
    print("Article count after program completed: ", articlecount_ending, file=f)
    print("Total modifications to database: ", total_changes, file=f)
    
    f.close()
    