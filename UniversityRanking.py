#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import os


def addSubject():
    global cur, conn
    allFiles = os.listdir()
    for file in allFiles:
        (ID, name, weight) = file[:len(file) - 4].split()
        cur.execute("INSERT INTO \"Subject\" VALUES (%s, %s, %s)", (ID, name, weight))
        conn.commit()


def addEvaluation():
    global cur, conn
    allFiles = os.listdir()
    for file in allFiles:
        IDSubject = file[:4]
        f = open(file)
        lines = f.readlines()
        grade = 0
        for line in lines:
            if line[0] == '\n':
                grade += 1
            else:
                (IDSchool, name) = line.split()
                try:                   
                    cur.execute("INSERT INTO \"School\" VALUES (%s, %s)", (IDSchool, name))
                    conn.commit()          
                except psycopg2.IntegrityError:
                    conn.rollback()
                try:
                    cur.execute("INSERT INTO \"Evaluation\" VALUES (%s, %s, %s)", (IDSchool, IDSubject, grade))
                    conn.commit()
                except psycopg2.IntegrityError:
                    conn.rollback()    
        f.close()

     
f = (100, 75, 30, 15, 9, 5, 3, 2, 1)
     
def calcSchool(IDSchool):
    global cur, conn
    cur.execute("SELECT \"IDSubject\", \"Grade\" FROM \"Evaluation\" WHERE \"IDSchool\" = (%s)", (IDSchool, ))
    (IDSubject, grade) = cur.fetchone()
    cur2 = conn.cursor()
    cur2.execute("SELECT \"Name\" FROM \"School\" WHERE \"ID\" = (%s)", (IDSchool, ))
    SchoolName = cur2.fetchone()[0]
    res = [IDSchool, SchoolName, 0, 0, 0, 0, 0, 0, 0]
    while True:
        cur2.execute("SELECT \"Weight\" FROM \"Subject\" WHERE \"ID\" = (%s)", (IDSubject, ))
        weight = cur2.fetchone()[0]
        if IDSubject < 700:     # 文
            res[2] += weight * f[grade]
        elif IDSubject < 800:   # 理
            res[3] += weight * f[grade]
        elif IDSubject < 900:   # 工
            res[4] += weight * f[grade]
        elif IDSubject < 1000:  # 农
            res[6] += weight * f[grade]
        elif IDSubject < 1100:  # 医
            res[5] += weight * f[grade]
        elif IDSubject < 1300:  # 管
            res[7] += weight * f[grade]
        else:                   # 艺
            res[8] += weight * f[grade]
        a = cur.fetchone()
        if a is None:
            break
        else:
            (IDSubject, grade) = a
    cur2.close()
    res = tuple(res)
    try:
        cur.execute("INSERT INTO \"Rank\" VALUES %s", (res, ))
    except psycopg2.IntegrityError:
        conn.rollback()
    conn.commit()
    

os.chdir("D:\\work\\python\\UniversityRanking\\data")
conn = psycopg2.connect(database = "postgres", user = "postgres", password = "SLCJUA", 
                        host = "localhost", port = "5432")
cur = conn.cursor()

cur3 = conn.cursor()
addSubject()
addEvaluation()
cur3.execute("SELECT \"ID\" FROM \"School\"")
IDSchool = cur3.fetchone()
while IDSchool is not None:
    calcSchool(IDSchool)
    IDSchool = cur3.fetchone()
cur3.close()

cur.close()
conn.close()
