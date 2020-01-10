#!/usr/bin/python3
import random, string
from xml.etree.ElementTree import Element, SubElement
from ElementTree_pretty import prettify
from xml_parser_header import Create_obj,Account,Position, Order
from response_obj import TransactionResponse,TransactionSubResponse

def randomword(length):
   letters = string.ascii_uppercase
   return ''.join(random.choice(letters) for i in range(length))

##response of create
def create_response(create):#create=Create_obj()
    top=Element('results')
    for child in create:
        if child.type=='account':
            if child.created:
                attributes={"id":str(child.account_id)}
                node=SubElement(top,'created',attributes)
            else:
                attributes={"id":str(child.account_id)}
                node=SubElement(top,'error',attributes)
                node.text=child.err

        elif child.type=='position':
            if child.created:
                attributes={"id":str(child.account_id),"sym":child.symbol}
                node=SubElement(top,'created',attributes)
            else:
                attributes={"id":str(child.account_id),"sym":child.symbol}
                node=SubElement(top,'error',attributes)
                node.text=child.err
    return prettify(top)




##response of transaction
#response
def transaction_response(response):
    top=Element('results')
    for child in response:
        if child.type=='order':
            if (child.success):
                attributes={"sym":child.symbol, "amount":str(child.amount), "limit":str(child.limit_price), "id":str(child.trans_id)}
                node=SubElement(top,'opened',attributes)
            else:
                attributes={"sym":child.symbol, "amount":str(child.amount), "limit":str(child.limit_price)}
                node=SubElement(top,'error',attributes)
                node.text=child.err
        if child.type == 'query' or child.type == 'cancel':
            attributes={"id":str(child.trans_id)}
            if(child.success):
                if child.type=='query':
                    node=SubElement(top,'status',attributes)
                if child.type=='cancel':
                    node=SubElement(top,'canceled',attributes)
                for grand_child in child.trans_resp:
                    if(grand_child.status=='open'):
                        sub_attributes = {"shares": str(grand_child.shares)}
                        subnode = SubElement(node, 'open', sub_attributes)
                    if(grand_child.status=='cancelled'):
                        sub_attributes = {"shares": str(grand_child.shares), "time": str(grand_child.time)}
                        subnode = SubElement(node, 'canceled', sub_attributes)
                    if(grand_child.status=='executed'):
                        sub_attributes = {"shares": str(grand_child.shares), "price": str(grand_child.price), "time": str(grand_child.time)}
                        subnode = SubElement(node, 'executed', sub_attributes)
            else:
                node=SubElement(top,'error',attributes)
                node.text=child.err
    return prettify(top)

"""
create_response() test
"""

# account1=Account('1',1000)
# account2=Account('2',2000)
# account2.create=False
# position1=Position('STOCK1','1',200)
# position2=Position('STOCK2','2',300)
# position2.create=False
# create=Create_obj()
# create.sequence.append(account1)
# create.sequence.append(account2)
# create.sequence.append(position1)
# create.sequence.append(position2)
# create_response(create)




"""
transaction_response() test
"""


# order1=Order('DUKE','3000','4000','4')
# order1.err='1'
# order2=Order('UNC','300','400','5')
# order2.success=False
# order2.status='cancelled'
# order2.err='2'
#
# trs1=TransactionSubResponse('open','300','300','300')
# trs2=TransactionSubResponse('canceled','20','20','20')
# trs3=TransactionSubResponse('executed','66','66','66')
# tr1=TransactionResponse('1','query')
# tr1.trans_resp.append(trs1)
# tr1.trans_resp.append(trs2)
# tr1.trans_resp.append(trs3)
# tr2=TransactionResponse('2','cancel')
# tr2.trans_resp.append(trs2)
# tr2.trans_resp.append(trs3)
# response=[]
# response.append(order1)
# response.append(order2)
# response.append(tr1)
# response.append(tr2)
# transaction_response(response)




def create_request():
    top=Element('create')
    attributes1={"id":str(random.randint(1,10001)),"balance":str(random.randint(1,10001))}
    SubElement(top, 'account', attributes1)
    attributes2={"sym":randomword(3)}
    node=SubElement(top,'symbol',attributes2)
    attributes3={"id":str(random.randint(1,10001))}
    node1=SubElement(node,'account',attributes3)
    node1.text=str(random.randint(1,10001))
    print(prettify(top))

def transaction_request():
    top=Element('transactions')
    attributes1={'id':str(random.randint(1,10001))}
    top.attrib=attributes1
    attributes2={'sym':randomword(3),'amount':str(random.randint(-10000,10001)),'limit':str(random.randint(1,10001))}
    SubElement(top,'order',attributes2)
    attributes3={'id':str(random.randint(1,10001))}
    SubElement(top,'query',attributes3)
    attributes4={'id':str(random.randint(1,10001))}
    SubElement(top,'cancel',{'id':str(random.randint(1,10001))})
    print(prettify(top))












