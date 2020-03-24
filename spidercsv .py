import csv
import sqlite3
import arrow

conn = sqlite3.connect('marathondb.sqlite')
cur= conn.cursor()

cur.execute('DROP TABLE IF EXISTS Racing')
cur.execute('DROP TABLE IF EXISTS Shoes')

# for date and time we will use the strftime function, so date, time and pace will become integers
# 1 for short means under 5km, instead 0 more than 5, the same if for after_2004

cur.execute('''CREATE TABLE Racing(
id INTEGER PRIMARY KEY,
date TEXT,
miles INTEGER,
shoebrand_id INTEGER,
time_min REAL,
pace_min REAL,
short INTEGER,
after_2004 INTEGER
)''')

cur.execute('''CREATE TABLE Shoes(
id INTEGER PRIMARY KEY,
shoes TEXT UNIQUE
)''')

#we will use the csv library

try:
    csvfile = open('Marathon.csv', 'r')
    csvread = csv.reader(csvfile, delimiter = ',')
except:
    print('Maybe you do not have the file')

fields = dict()
shoes = dict()

for row in csvread:

    if row[0] == '':
        row.pop(0) # the first field is useless
        for field in row:
            fields[field] = row.index(field) # we are a dictionary with the various fields for doing it more easily
        continue

    row.pop(0) # the first field is useless

    shoe = row[fields['ShoeBrand']]
    
    if shoe not in shoes and shoe != '': # if we didn't extract it already
        cur.execute('INSERT OR IGNORE INTO Shoes(shoes) VALUES(?)', (shoe,))
        conn.commit()
        id = cur.lastrowid
        shoes[shoe] = id # it will be useful for the foreign key

    date = row[fields['Date']]
    bdate = arrow.get(date, 'M/D/YYYY')
    bdate = bdate.format('YYYY-MM-DD')
    miles = row[fields['Miles']]
    shoebrand_id = None
    if shoe != '':
        shoebrand_id = shoes[shoe]
    time_min = row[fields['TimeMin']]
    pace_min = row[fields['PaceMin']]
    short = row[fields['Short']]
    after_2004 = row[fields['After2004']]

    cur.execute('''
                INSERT INTO Racing(date,miles,shoebrand_id,time_min,pace_min,short,after_2004) VALUES(datetime(?),?,?,?,?,?,?)''',
                 (bdate,miles,shoebrand_id,time_min,pace_min,short,after_2004,))

    conn.commit()

print(shoes)
cur.close()
