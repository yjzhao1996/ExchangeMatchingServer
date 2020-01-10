#!/usr/bin/python3
import psycopg2
import sys

retry=5
while retry:
    try:
        database='exchange_matching'
        conn = psycopg2.connect(database='exchange_matching',\
                                user='postgres', password='passw0rd',\
                                host='db', port='5432')

        print("Opened database %s successfully." % database)
        break
    except:
        print("Failed to connect to database %s.", database)
        time.sleep(3)
        retry-=1

try:    
    cur = conn.cursor()
    cur.execute('''DROP TABLE IF EXISTS Orders CASCADE;''')
    cur.execute('''DROP TYPE IF EXISTS order_status CASCADE;''')
    cur.execute('''DROP TABLE IF EXISTS Positions CASCADE;''')
    cur.execute('''DROP TABLE IF EXISTS Accounts CASCADE;''')

    conn.commit()
except:
    print ('Error: Drop tables')
    pass

try:    
    cur = conn.cursor()
    cur.execute('''CREATE TABLE Accounts (
    account_id int PRIMARY KEY,
    balance real NOT NULL
    );''')

    conn.commit()
except:
    print ('Table Accounts may already exist.')
    pass

try:
    cur.execute('''CREATE TABLE Positions (
    position_id SERIAL PRIMARY KEY,
    symbol varchar(10) NOT NULL,
    amount real NOT NULL, /* positive real number */
    account_id int NOT NULL,
    FOREIGN KEY(account_id) REFERENCES Accounts(account_id) ON DELETE CASCADE
    );''')
    conn.commit()
except:
    print ("Table Positions may already exist.")
    pass

try:
    cur.execute('''CREATE TYPE order_status \
    AS ENUM ('open', 'cancelled','executed');''')
    conn.commit()
except:
    print ("order_status enum may already exist.")
    pass

try:
    cur.execute('''
    CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    trans_id int NOT NULL,
    symbol varchar(10) NOT NULL,
    amount real NOT NULL, /* +ve or -ve real number -ve=>sell */
    limit_price real NOT NULL, /* positive real number */
    account_id int NOT NULL,
    time int NOT NULL, /* time since epoch */
    status order_status NOT NULL DEFAULT 'open',
    /* need to add time */
    FOREIGN KEY(account_id) REFERENCES Accounts(account_id) ON DELETE CASCADE /* ideally on delete set status to cancelled */
    );''')
    conn.commit()
except:
    print ("Table Orders may already exist.")
    pass

conn.close()
