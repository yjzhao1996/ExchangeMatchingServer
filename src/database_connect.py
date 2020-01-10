#!/usr/bin/python3
"""
March 28, 2019
Connect to database and perform all required sql queries
"""
_author_ = "Prathikshaa Rangarajan"
_maintainer_= "Prahikshaa Rangarajan"

import psycopg2
import sys
import threading
import time


from xml_parser_header import *
from response_obj import *

# sys.path.append('..')
# import parser.xml_parser_header

# if __name__ == "__main__" and __package__ is None:
#         from sys import path
#         from os.path import dirname as dir

#         path.append(dir(path[0]))
#         from src.parser.xml_parser_header import Accounts

# from parser.xml_parser_header import Accounts

# Connects to the database and returns database connection object

lock_table={}
account_lock=threading.Lock()

def connect():
    database = 'exchange_matching'
    retry = 5
    while retry:
        try:
            conn = psycopg2.connect(database='exchange_matching', \
                                    user='postgres', password='passw0rd', \
                                    host='db', port='5432')
            print("Opened database %s successfully." % database)
            break
        except:
            print("Failed to connect to database ", database)
            time.sleep(3)
            retry = retry - 1
            print("retries left: ", retry)
    return conn


#-------------------------------Creations--------------------------------#

'''
 Takes a connection object and account creation fields
 creates account, if successful, returns connection object
 else raises
 -- psycopg2.IntegrityError
 -- ValueError
'''
#test_account= Account()
#create_account(connect(), test_account)

# thread-safe
# basic exception guarantee
def create_account(conn, account):
    global account_lock
    try:
        account_id_int = int(account.account_id)
        balance_float = float(account.balance)
    except: # ValueError
        account.created = False
        account.err = "Invalid Account Format" + sys.exc_info()
        return account

    try:
        account_lock.acquire()
        cur = conn.cursor()
        cur.execute('''INSERT INTO Accounts
        (account_id, balance)
        VALUES (%s, %s);'''
                    , (account.account_id, account.balance))

        conn.commit()


    except psycopg2.IntegrityError:
        account.created = False
        account.err = "Account already exists."
    except:
        account.created = False
        account.err = "Account creation failed due to unknown reasons." + sys.exc_info()
        # print ('Failed to create account', sys.exc_info())
        pass
    conn.commit()
    account_lock.release()
    return account

def test_account_creation():
    try:
        db_conn = connect()
        create_account(db_conn, 10, 1234.45)
        db_conn.close()
    except psycopg2.IntegrityError:
        print("Account already exists")
        pass
    except ValueError:
        print("Invalid Account Format")
        pass
    except:
        print("Account creation failed due to unknown reasons.")
        pass
    pass


#test_position = Position()
#create_position(connect(), test_position)

# thread-safe
# basic exception guarantee
def create_position(conn, position):
    global lock_table
    try:
        amount_float = float(position.amount)
        account_id_int = int(position.account_id)
    except: # ValueError
        position.created = False
        position.err = "Invalid position format" + sys.exc_info()
        return position

    try:
        cur = conn.cursor()

    # read-modify write start
    # lock(symbol)
        if position.symbol in lock_table.keys():
            symbol_lock=lock_table[position.symbol]
            symbol_lock.acquire()
        else:
            lock_table[position.symbol]=threading.Lock()
            symbol_lock=lock_table[position.symbol]
            symbol_lock.acquire()
        cur.execute('''SELECT COUNT(*) FROM Positions
        WHERE symbol = %s AND account_id = %s''', (position.symbol, position.account_id))
        row = cur.fetchone()

        # update if position already exists
        if row[0] == 1:
            cur.execute('''UPDATE Positions SET amount = amount + %s 
            WHERE symbol = %s AND account_id = %s''', (position.amount, position.symbol, position.account_id))
            pass
        # create new, if no such position exists
        else:
            cur.execute('''INSERT INTO Positions (symbol, amount, account_id) 
            VALUES(%s, %s, %s)''', (position.symbol, position.amount, position.account_id))
            pass

        conn.commit()

    # unlock(symbol)
    # read-modify-write end

    except psycopg2.IntegrityError:
        # raise
        position.created = False
        position.err = "Account corresponding to database may not exist." # + sys.exc_info()
    except:
        # print ('Failed to create position', sys.exc_info())
        # pass
        position.created = False
        position.err = "Position creation failed due to unknown reasons." # + sys.exc_info()
    conn.commit()
    symbol_lock.release()
    return position

def test_position_creation():
    try:
        db_conn = connect()
        create_position(db_conn, "ac", 100, 12)
        db_conn.close()
    except ValueError:
        print("Invalid position format")
        pass
    except:
        print("Postion creation failed due to unknown reasons")
        pass
           
#-----------------------------------------Transactions-----------------------------------------#

# thread-safe
# basic exception guarantee
def create_order(conn, order, account_id):
    # type checking inputs
    try:
        amount_float = float(order.amount)
        account_id_int = int(account_id)
        limit_price_float = float(order.limit_price)
    except: # ValueError
        order.success = False
        order.err = "Invalid order creation inputs"
        return order

    buy = True
    if order.amount < 0 :
        buy = False
        pass

    print (order.time)
    # """
    # Buy Order
    # Reduce balance in Accounts
    # """
    if buy is True:
        print('is a buy order')
        order = create_buy_order(conn, order, account_id)
    # """
    # Sell Order
    # Reduce amount in positions
    # """

    else:
        print('is a sell order')
        order = create_sell_order(conn, order, account_id)
        pass
    if not order.success:
        return order
    match = True
    while match:
        match = match_order(conn, order.symbol)
        # symbol_lock.release()
    return order

# thread-safe
# basic exception guarantee
def create_buy_order(conn, order, account_id):
    global account_lock
    try:
        cur = conn.cursor()
    # read-modify-write start
    # lock(Accounts)
        account_lock.acquire()
        cur.execute('''SELECT balance FROM Accounts WHERE account_id = %s''', (account_id,))
        row = cur.fetchone()
        if not row:
            order.success = False
            order.err = 'Account does not exist'
            account_lock.release()
            return order
        balance = row[0]
        share_price = order.limit_price * order.amount
        if balance < share_price:
            # Insufficient funds error
            order.success = False
            order.err = 'Insufficient Funds'
            account_lock.release()
            return order
        
        cur.execute('''UPDATE Accounts SET balance = balance-%s WHERE account_id = %s''', (share_price, account_id))
        cur.execute('''INSERT INTO Orders (trans_id, symbol, amount, limit_price, account_id, time) VALUES(%s, %s, %s, %s, %s, %s)''', (order.trans_id, order.symbol, order.amount, order.limit_price, account_id, order.time))

    # unlock(Accounts)
    # read-modify-write end
        conn.commit()

    except psycopg2.IntegrityError:
        # raise
        order.success = False
        order.err = 'Failed to create order ' + sys.exc_info()
        pass
    except:
        # print ('Failed to create buy order', sys.exc_info())
        order.success = False
        order.err = 'Failed to create order '  + sys.exc_info()
        pass
    conn.commit()
    account_lock.release()
    return order

# thread-safe
# basic exception guarantee
def create_sell_order(conn, order, account_id):
    global lock_table
    try:
        cur = conn.cursor()
    # read-modify-write start
    # lock(Positions)
        if order.symbol in lock_table.keys():
            symbol_lock = lock_table[order.symbol]
            symbol_lock.acquire()
        else:
            lock_table[order.symbol] = threading.Lock()
            symbol_lock = lock_table[order.symbol]
            symbol_lock.acquire()

        cur.execute('''SELECT COUNT(*) FROM Positions 
        WHERE symbol = %s AND account_id = %s AND amount > (-%s)''', (order.symbol, account_id, order.amount))
        row = cur.fetchone()
        if not row:
            order.success = False
            order.err = 'No such position to sell from'
            symbol_lock.release()
            return order
        position_count = row[0]
        if position_count != 1:
            # Insufficient Shares to sell error
            order.success = False
            order.err = 'Insufficient shares to sell'
            symbol_lock.release()
            return order
        cur.execute('''UPDATE Positions SET amount = amount + %s 
       WHERE account_id = %s AND symbol = %s''', (order.amount, account_id, order.symbol))
        cur.execute('''INSERT INTO Orders (trans_id, symbol, amount, limit_price, account_id, time) VALUES(%s, %s, %s, %s, %s, %s)''', (order.trans_id, order.symbol, order.amount, order.limit_price, account_id, order.time))

    # unlock(Positions)
    # read-modify-write end
        conn.commit()


    except psycopg2.IntegrityError:
        # raise
        order.success = False
        order.err = "Database Error: Invalid account or symbol or combination thereof " + sys.exc_info()
        pass
    
    except:
        # print('Failed to create sell order', sys.exc_info())
        order.success = False
        order.err = "Failed to create order " + sys.exc_info()
        pass

    conn.commit()
    symbol_lock.release()
    return order

def test_order():
    account_id = 12

    sym = "abc"
    amount = 1000
    limit_price = 125
    
    trans_id = 2
    order = Order(sym, amount, limit_price, trans_id)
    create_order(connect(), order, account_id)

    print(order.success)
    print('Error:')
    print(order.err)
    return

#test_order()

# read-only
# no locks
def query_order(conn, query_obj):
    query_resp = TransactionResponse(query_obj.trans_id, 'query')

    try:
        trans_id = int(query_obj.trans_id)
    except:
        query_resp.success = False
        query_resp.err = 'Invalid format of transaction id'
        return query_resp

    try:
        cur = conn.cursor()
        cur.execute('''SELECT status, amount, limit_price, time FROM Orders WHERE trans_id = %s;''', (trans_id,))
        rows = cur.fetchall()
        if not rows:
            query_resp.success = False
            query_resp.err = 'No orders found with given transaction id'
            return query_resp

        for row in rows:
            epoch_time = int(time.time()) - row[3]
            resp = TransactionSubResponse(row[0], row[1], row[2], epoch_time)
            query_resp.trans_resp.append(resp)
    except psycopg2.IntegrityError:
        # raise
        query_resp.success = False
        query_resp.err = "Database Error" + sys.exc_info()
        pass
    
    except:
        query_resp.success = False
        query_resp.err = "Failed to query transaction ID " + sys.exc_info()
        pass

    conn.commit()
    pass
    return query_resp

def test_query():
    query_obj = Query(10000000)
    resp = query_order(connect(), query_obj)
    # for row in resp.trans_resp:
    #     print(row)
    #     pass

    # if not resp.success:
    #     print(resp.err)
    #     pass
    print(resp)

# test_query()

# used to credit money to accounts on successful sell/buy or refunded buy
# not thread safe
def refund(conn, account_id, refund_amount):
    try:
        cur = conn.cursor()
        cur.execute('''UPDATE Accounts SET balance = balance + %s
                    WHERE account_id = %s''', (refund_amount, account_id))
        print('refunded to account',  refund_amount, account_id)
        conn.commit()
    except psycopg2.IntegrityError:
        print('Failed to deposit money to account')
        print (sys.exc_info())
        throw
    except:
        print('Failed to deposit money to account')
        print (sys.exc_info())
        throw
    return



def cancel_order(conn, cancel_obj):
    global account_lock
    global lock_table

    cancel_resp = TransactionResponse(cancel_obj.trans_id, 'cancel')

    try:
        trans_id = int(cancel_obj.trans_id)
    except:
        cancel_resp.success = False
        cancel_resp.err = 'Invalid format of transaction id'
        return cancel_resp

    try:
        cur = conn.cursor()
        cur.execute('''SELECT symbol FROM Orders WHERE trans_id=%s;''',(cancel_obj.trans_id,))
        symbols = cur.fetchall()
        if not symbols:
            cancel_resp.success = False
            cancel_resp.err = 'No Orders with given trans_id'
            return cancel_resp
        sym_set = set()
        for symbol in symbols:
            # print(symbol[0])
            sym_set.add(symbol[0])
            pass

        for sym in sym_set:
            lock_table[sym].acquire()
            pass

        cur.execute('''UPDATE Orders SET Status='cancelled' 
            WHERE trans_id=%s AND Status = 'open'
            RETURNING symbol, amount, limit_price, account_id;''', (trans_id,))
        for sym in sym_set:
            lock_table[sym].release()
            pass

        cancelled_orders = cur.fetchall()
        for cancelled_order in cancelled_orders:
            print('cancelled: ', cancelled_order)
            symbol = cancelled_order[0]
            amount = cancelled_order[1]
            limit_price = cancelled_order[2]
            account_id = cancelled_order[3]
            if amount < 0:
                position = Position(symbol, account_id, abs(amount))
                create_position(conn, position) # thread-safe -- uses symbol_lock
                pass
            else:
                refund_amount = limit_price * amount
                account_lock.acquire()
                refund(conn, account_id, refund_amount) # not thread-safe
                account_lock.release()
                pass
            pass
        conn.commit()

        
        cur.execute('''SELECT status, amount, limit_price, time FROM Orders WHERE trans_id = %s;''', (trans_id,))
        rows = cur.fetchall()
        if not rows:
            cancel_resp.success = False
            cancel_resp.err = 'No orders found with given transaction id'
            return cancel_resp
        
        for row in rows:
            epoch_time = int(time.time()) - row[3]
            resp = TransactionSubResponse(row[0], row[1], row[2], epoch_time)
            cancel_resp.trans_resp.append(resp)

    except psycopg2.IntegrityError:
        # raise
        cancel_resp.success = False
        cancel_resp.err = "Database Error" + sys.exc_info()
        pass
    
    except:
        cancel_resp.success = False
        cancel_resp.err = "Failed to cancel transaction ID " + sys.exc_info()
        pass

    return cancel_resp

def test_cancel():
    cancel_obj = Cancel(4)
    resp = cancel_order(connect(), cancel_obj)
    # for row in resp.trans_resp:
    #     print(row)
    #     pass

    # if not resp.success:
    #     print(resp.err)
    #     pass
    print(resp)


'''
Match all orders on a given symbol.
Uses symbol lock
account balance update requires a global lock
'''
# safe for accounts based trasnsactions
# requires a symbol lock -- thread unsafe for symbols

def match_order(conn, symbol):
    global account_lock
    global lock_table
    # get highest buy order
    # get lowest sell order
    # if they match
    # how many shares in each matched
    # what is the exec price - price of lower trans_id
    # update money in both accounts
    # update Positions in buyer account

    # lock(symbol)
    if symbol in lock_table.keys():
        symbol_lock=lock_table[symbol]
        symbol_lock.acquire()
    else:
        return False
    try:
        match = False
        cur = conn.cursor()
        cur.execute('''SELECT trans_id, amount, limit_price, account_id, time  FROM Orders 
        WHERE symbol = %s AND status = 'open' AND amount > 0 AND
        limit_price = (SELECT max(limit_price) FROM Orders WHERE amount>0 AND symbol = %s);''', (symbol,symbol))
        open_buy_orders = cur.fetchall()

        if not open_buy_orders:
            print('No buy orders open for symbol')
            symbol_lock.release()
            return match

        # debug print
        for open_buy_order in open_buy_orders:
            print(open_buy_order)
            pass

        # break ties in buy order based on trans_id [0]
        buy_match = sorted(open_buy_orders, key = lambda i: i[0], reverse = False)[0]
        print('buy match: ', buy_match)

        cur.execute('''SELECT trans_id, amount, limit_price, account_id, time FROM Orders
                    WHERE symbol=%s AND status = 'open' AND amount < 0 AND
                    limit_price = (SELECT min(limit_price) FROM Orders WHERE amount<0 AND symbol = %s);''', (symbol, symbol))
        open_sell_orders = cur.fetchall()

        if not open_sell_orders:
            print('No sell orders open for symbol')
            symbol_lock.release()
            return match

        # debug print
        for open_sell_order in open_sell_orders:
            print(open_sell_order)
            pass

        # break ties in sell order based on trans_id [0]
        sell_match = sorted(open_sell_orders, key = lambda i: i[0], reverse = False)[0]
        print('sell match: ', sell_match)

        if(buy_match[2] >= sell_match[2]):
            match = True

            # determine exec_price from limit_price [2]
            if buy_match[0] <= sell_match[0]:
                buyer_price = True
                exec_price = buy_match[2]
                pass
            else:
                buyer_price = False
                exec_price = sell_match[2]
                pass

            # determine exec_shares from amount [1]
            if buy_match[1] <= abs(sell_match[1]):
                buyer_shares = True
                exec_shares = buy_match[1]
                pass
            else:
                buyer_shares = False
                exec_shares = abs(sell_match[1])
                pass
            
            transac_cost = exec_price * exec_shares
            if buyer_price:
                # no credit to buyer account
                pass
            else:
                # credit (buyer_price - exec_price) * exec_shares to buyer account
                refund_amount = (buy_match[2] - exec_price) * exec_shares
                if refund_amount != 0:
                # lock(Accounts)
                    account_lock.acquire()
                    refund(conn, buy_match[3], refund_amount)
                    account_lock.release()
                # unlock(Accounts)
                pass

           
            # credit seller account with transac_cost
        # lock(Accounts)
            print('seller account_id = ', sell_match[3])
            account_lock.acquire()
            refund(conn, sell_match[3], transac_cost)
            account_lock.release()
        # unlock(Accounts)

        if buyer_shares:
            print('buyer bought all %s shares.', exec_shares)
            cur.execute('''UPDATE Orders SET status='executed' 
                WHERE trans_id = %s''', (buy_match[0],))
            cur.execute('''UPDATE Orders SET amount = amount + %s 
                WHERE trans_id = %s''', (exec_shares, sell_match[0]))
            cur.execute('''INSERT INTO Orders(trans_id, symbol, amount, limit_price, account_id, time, status)
                VALUES(%s, %s, %s, %s, %s, %s, %s)''',
                (sell_match[0], symbol, (-exec_shares), sell_match[2], sell_match[3], sell_match[4] , 'executed'))
            pass
        else:
            print('seller sold all %s shares', exec_shares)
            cur.execute('''UPDATE Orders SET status='executed' 
                WHERE trans_id = %s''', (sell_match[0],))
            cur.execute('''UPDATE Orders SET amount = amount - %s 
                WHERE trans_id = %s''', (exec_shares, buy_match[0]))
            cur.execute('''INSERT INTO Orders(trans_id, symbol, amount, limit_price, account_id, time, status)
                VALUES(%s, %s, %s, %s, %s, %s, %s)''',
                (buy_match[0], symbol, exec_shares, buy_match[2], buy_match[3], buy_match[4],  'executed'))
            pass

        symbol_lock.release()

        # insert exec_shares into buyer account Positions
        position = Position(symbol, buy_match[3], exec_shares)
        create_position(conn, position)

        print('num of shares = ', exec_shares)
        print('transaction price = ', transac_cost)

        conn.commit()

    except psycopg2.IntegrityError:
        print('Database Error: Order matching failed')
        match = False
        return match
    except:
        print (sys.exc_info())
        match = False
        return match
    return match

# match_order(connect(), 'aa')
