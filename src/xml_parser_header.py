#!/usr/bin/python3
"""
March 26, 2019
Creates the required objects that the XML parser reads into.
"""
_author_ = "Yanjia Zhao"
_maintainer_ = "Prathikshaa Rangarajan"

import xml.etree.ElementTree as ET
import time

order_id=1

"""
classes for Create_obj
"""

class Account:
    def __init__(self, account_id, balance):
        self.account_id = account_id # should be string
        self.balance = balance
        self.type = 'account'
        self.created = True ##True means created succuss, FALSE means error; True as default
        self.err = ""

    """
    print function
    """
    def __repr__(self):
        print('Account ID: ', self.account_id)
        print('Account Balance: ', self.balance)

        return ''

        
class Position:
    def __init__(self, symbol, account_id, amount):
        self.symbol = symbol
        self.account_id = account_id #the array of Account objects, should be string
        self.amount = amount # no. of shares of the symbol to be added to the account
        self.type = 'position'
        self.created = True ##True means created succuss, FALSE means error; True as default
        self.err = ""
    """
    print function
    """
    def __repr__(self):
        print('Symbol: ', self.symbol)
        print('Account ID: ', self.account_id)
        print('Num. of shares of symbol: ', self.amount)
            
        return ''

        
class Create_obj:
    def __init__(self):
        self.type = 'create'
        self.sequence=[]

    """
    print function
    """        
    def __repr__(self):
        print('Create:')
        for index in range (0, len(self.sequence)):
            print(self.sequence[index])
            pass    
        
       
        return ''


"""
classes for Transaction_obj
"""
        
class Order:
    def __init__(self, symbol, amount, limit_price, trans_id):
        self.trans_id = trans_id
        self.symbol = symbol
        self.amount = amount
        self.limit_price = limit_price
        self.time  = int(time.time())
        self.type = 'order'
        self.success = True
        self.status = 'open' # open , executed, cancelled
        self.err = ""

    """
    print function
    """        
    def __repr__(self):

        print('Order:')
        print('Symbol: ', self.symbol)
        print('Amount: ', self.amount)
        print('Limit: ', self.limit_price)

        return ''
        
class Query:
    def __init__(self, trans_id):
        self.trans_id = trans_id
        self.type = 'query'

    """
    print function
    """        
    def __repr__(self):
        print('Query ID: ', self.trans_id)
        return ''

        
class Cancel:
    def __init__(self, trans_id):
        self.trans_id = trans_id
        self.type = 'cancel'

    """
    print function
    """        
    def __repr__(self):
        print('Cancel ID: ', self.trans_id)
        return ''

        
class Transaction_obj:
    def __init__(self, account_id):
        self.type = 'transac'
        self.account_id = account_id
        self.sequence = []

    """
    print function
    """        
    def __repr__(self):
        print('Transaction ID: ', self.id)

        for element in self.sequence:
            print(element)
            
        return ''

def parse_xml(recv_string):
    global order_id
    root=ET.fromstring(recv_string)
    #process create object here
    if root.tag=='create':
        create_obj=Create_obj()
        for child in root:
            if child.tag=='account':
                id1=int(child.attrib.get('id'))
                balance=float(child.attrib.get('balance'))
                account=Account(id1,balance)
                create_obj.sequence.append(account)
            elif child.tag=='symbol':
                symbol=child.attrib.get('sym')
                for grandchild in child:
                    position_account_id=grandchild.attrib.get('id')
                    num=grandchild.text
                    pos = Position(symbol, int(position_account_id), float(num))
                    create_obj.sequence.append(pos)
        return create_obj

    #process transaction object here
    if root.tag=='transactions':
        account_id=root.attrib.get('id')
        transaction_obj=Transaction_obj(int(account_id))
        for child in root:
            if child.tag=='order':
                symbol=child.attrib.get('sym')
                amount=child.attrib.get('amount')
                limit = child.attrib.get('limit')
                order=Order(symbol,float(amount),float(limit),order_id)
                order_id+=1
                transaction_obj.sequence.append(order)
                pass

            elif child.tag=='query':
                trans_id = child.attrib.get('id')
                query=Query(int(trans_id))
                transaction_obj.sequence.append(query)
                pass

            elif child.tag=='cancel':
                trans_id = child.attrib.get('id')
                cancel=Cancel(int(trans_id))
                transaction_obj.sequence.append(cancel)
                pass
            pass

        return transaction_obj

    #     print("Transaction_Obj ID: ", transaction_obj.id)
    #     for order in transaction_obj.order:
    #         print(order.symbol,order.amount,order.limit)
    #     for query in transaction_obj.query:
    #         print(query.id)
    #     for cancel in transaction_obj.cancel:
    #         print(cancel.id)
    #         pass
    #     pass
    # pass


