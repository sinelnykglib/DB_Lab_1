import csv
import psycopg2
import time
import py7zr
import pandas as pd
import math
n_row = 10000

# Завантажуємо та розпаковуємо файл з даними за 2019 рік
filename_1 = 'OpenDataZNO2019.7z'

with py7zr.SevenZipFile(filename_1, 'r') as archive:
    archive.extractall()

# Завантажуємо та розпаковуємо файл з даними за 2020 рік
filename_2 = 'OpenDataZNO2020.7z'

with py7zr.SevenZipFile(filename_2, 'r') as archive:
    archive.extractall()

start_time = time.time()
print('Start time:', time.strftime('%H:%M:%S'))
# Підключення до бази даних
def connect():
    while True:
        try:
            conn = psycopg2.connect(
                host="db",
                database="db_labs",
                user="postgres",
                password="23102002Papa"
                )
            cur = conn.cursor()
            break
        except:
            pass
    return conn, cur


def create_table():
    conn, cur = connect() 
    while True:
        try:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS zno_data(
                    Year INT,
                    OUTID VARCHAR(1000) PRIMARY KEY,
                    Birth DECIMAL,
                    SEXTYPENAME CHAR(10),
                    REGNAME VARCHAR(1000),
                    AREANAME VARCHAR(1000),
                    TERNAME VARCHAR(1000),
                    REGTYPENAME VARCHAR(1000),
                    TerTypeName VARCHAR(1000),
                    ClassProfileNAME VARCHAR(1000),
                    ClassLangName VARCHAR(1000),
                    EONAME VARCHAR(1000),
                    EOTYPENAME VARCHAR(1000),
                    EORegName VARCHAR(1000),
                    EOAreaName VARCHAR(1000),
                    EOTerName VARCHAR(1000),
                    EOParent VARCHAR(1000),
                    UkrTest VARCHAR(1000),
                    UkrTestStatus VARCHAR(1000),
                    UkrBall100 DECIMAL,
                    UkrBall12 DECIMAL,
                    UkrBall DECIMAL,
                    UkrAdaptScale INT,
                    UkrPTName VARCHAR(1000),
                    UkrPTRegName VARCHAR(1000),
                    UkrPTAreaName VARCHAR(1000),
                    UkrPTTerName VARCHAR(1000),
                    histTest VARCHAR(1000),
                    HistLang VARCHAR(1000),
                    histTestStatus VARCHAR(1000),
                    histBall100 DECIMAL,
                    histBall12 DECIMAL,
                    histBall DECIMAL,
                    histPTName VARCHAR(1000),
                    histPTRegName VARCHAR(1000),
                    histPTAreaName VARCHAR(1000),
                    histPTTerName VARCHAR(1000),
                    mathTest VARCHAR(1000),
                    mathLang VARCHAR(1000),
                    mathTestStatus VARCHAR(1000),
                    mathBall100 DECIMAL,
                    mathBall12 DECIMAL,
                    mathBall DECIMAL,
                    mathPTName VARCHAR(1000),
                    mathPTRegName VARCHAR(1000),
                    mathPTAreaName VARCHAR(1000),
                    mathPTTerName VARCHAR(1000),
                    physTest VARCHAR(1000),
                    physLang VARCHAR(1000),
                    physTestStatus VARCHAR(1000),
                    physBall100 DECIMAL,
                    physBall12 DECIMAL,
                    physBall DECIMAL,
                    physPTName VARCHAR(1000),
                    physPTRegName VARCHAR(1000),
                    physPTAreaName VARCHAR(1000),
                    physPTTerName VARCHAR(1000),
                    chemTest VARCHAR(1000),
                    chemLang VARCHAR(1000),
                    chemTestStatus VARCHAR(1000),
                    chemBall100 DECIMAL,
                    chemBall12 DECIMAL,
                    chemBall DECIMAL,
                    chemPTName VARCHAR(1000),
                    chemPTRegName VARCHAR(1000),
                    chemPTAreaName VARCHAR(1000),
                    chemPTTerName VARCHAR(1000),
                    bioTest VARCHAR(1000),
                    bioLang VARCHAR(1000),
                    bioTestStatus VARCHAR(1000),
                    bioBall100 DECIMAL,
                    bioBall12 DECIMAL,
                    bioBall DECIMAL,
                    bioPTName VARCHAR(1000),
                    bioPTRegName VARCHAR(1000),
                    bioPTAreaName VARCHAR(1000),
                    bioPTTerName VARCHAR(1000),
                    geoTest VARCHAR(1000),
                    geoLang VARCHAR(1000),
                    geoTestStatus VARCHAR(1000),
                    geoBall100 DECIMAL,
                    geoBall12 DECIMAL,
                    geoBall DECIMAL,
                    geoPTName VARCHAR(1000),
                    geoPTRegName VARCHAR(1000),
                    geoPTAreaName VARCHAR(1000),
                    geoPTTerName VARCHAR(1000),
                    engTest VARCHAR(1000),
                    engTestStatus VARCHAR(1000),
                    engBall100 DECIMAL,
                    engBall12 DECIMAL,
                    engDPALevel VARCHAR(1000),
                    engBall DECIMAL,
                    engPTName VARCHAR(1000),
                    engPTRegName VARCHAR(1000),
                    engPTAreaName VARCHAR(1000),
                    engPTTerName VARCHAR(1000),
                    fraTest VARCHAR(1000),
                    fraTestStatus VARCHAR(1000),
                    fraBall100 DECIMAL,
                    fraBall12 DECIMAL,
                    fraDPALevel VARCHAR(1000),
                    fraBall DECIMAL,
                    fraPTName VARCHAR(1000),
                    fraPTRegName VARCHAR(1000),
                    fraPTAreaName VARCHAR(1000),
                    fraPTTerName VARCHAR(1000),
                    deuTest VARCHAR(1000),
                    deuTestStatus VARCHAR(1000),
                    deuBall100 DECIMAL,
                    deuBall12 DECIMAL,
                    deuDPALevel VARCHAR(1000),
                    deuBall DECIMAL,
                    deuPTName VARCHAR(1000),
                    deuPTRegName VARCHAR(1000),
                    deuPTAreaName VARCHAR(1000),
                    deuPTTerName VARCHAR(1000),
                    spaTest VARCHAR(1000),
                    spaTestStatus VARCHAR(1000),
                    spaBall100 DECIMAL,
                    spaBall12 DECIMAL,
                    spaDPALevel VARCHAR(1000),
                    spaBall DECIMAL,
                    spaPTName VARCHAR(1000),
                    spaPTRegName VARCHAR(1000),
                    spaPTAreaName VARCHAR(1000),
                    spaPTTerName VARCHAR(1000)
                );
            ''')
            print('Table is created.')
            conn.commit()
            break
        except psycopg2.OperationalError:
            print("Reconnection...")
            conn, cur = connect() 


def custom_csv_reader(file):
    for row in file:
        yield row.replace(',', '.').replace("'", "`")


def float_check(a):
    try:
        if a !='null':
            float(a)
        return True
    except:
        return False


def row_to_query(row, year):
    tmp = str(year)
    for x in row:
        if float_check(x):
            tmp += f', {x}'
        else:
            tmp += f", '{x}'"

    query = f'''INSERT INTO zno_data 
     VALUES({tmp}) ON CONFLICT DO NOTHING;'''
    return query



def insert_data(filename, year):
    conn, cur = connect()
    left = []
    flag = False
    i = -1
    with open(filename, 'r', encoding='Windows-1251') as file:
        rows = csv.reader(custom_csv_reader(file), delimiter=';')
        for row in rows:
            if i == -1:
                i += 1
                continue
            if i > n_row:
                break
            query = row_to_query(row, year)
            left.append(query)
            while True:
                try:
                    if flag:
                        cur.execute(' \n'.join(left))
                        flag = False
                        i += len(left)
                    else:
                        cur.execute(query)
                        i += 1
                except psycopg2.OperationalError:
                    print("Reconnection...")
                    conn, cur = connect() 
                    continue
                try:
                    if i % 100 == 0:
                        conn.commit()
                        left = []
                        print(f"{i} rows inserted.")
                    break
                except psycopg2.OperationalError:
                    flag = True
                    print("Reconnection...")
                    conn, cur = connect() 
        while True:
            try:
                conn.commit()
                left = []
                print(f"{len(left)} rows inserted.")
                break
            except psycopg2.OperationalError:
                print("Reconnection...")
                conn, cur = connect() 
            cur.execute(' \n'.join(left))


def result():
    conn, cur = connect()

    query1 = """
                SELECT REGNAME, AVG(PhysBall100)
                FROM zno_data
                WHERE PhysTestStatus = 'Зараховано' AND Year = 2019
                GROUP BY REGNAME
                """
    query2 = """
                SELECT REGNAME, AVG(PhysBall100)
                FROM zno_data
                WHERE PhysTestStatus = 'Зараховано' AND Year = 2020
                GROUP BY REGNAME
                """


    # виконання запиту для 2019 року
    cur.execute(query1)
    results_2019 = cur.fetchall()
    print(results_2019)
    # виконання запиту для 2020 року
    cur.execute(query2)
    results_2020 = cur.fetchall()
    print(results_2020)

    # запис результатів до CSV-файлу
    with open('../AVG_Phys.csv', mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Region', 'AVG Phys in 2019', 'AVG Phys in 2020'])
        for row_2019, row_2020 in zip(results_2019, results_2020):
            region = row_2019[0]
            avg_2019 = row_2019[1]
            avg_2020 = row_2020[1]
            writer.writerow([region, avg_2019, avg_2020])

    print("The results have been saved to 'AVG_Phys.csv' file")

    elapsed_time = round(time.time() - start_time, 2)
    print('Working time:', elapsed_time)

if __name__ == "__main__":
    conn = None
    cur = None
    conn, cur = connect()

    try:
        create_table()
        print("Creating table...")
        
        insert_data('Odata2019File.csv', 2019)
        insert_data('Odata2020File.csv', 2020)

        result()
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()