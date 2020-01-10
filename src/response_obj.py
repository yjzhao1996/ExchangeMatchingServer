class TransactionSubResponse:
    def __init__(self, status, shares, price, time):
        self.status = status
        self.shares = shares
        self.price = price
        self.time = time
        pass

    # print Transaction Sub Response
    def __repr__(self):
        print('Status: ' + self.status)
        print('Amount of shares: ', self.shares)
        print('Price: ', self.price)
        print('Time: ', self.time)
        return ''

class TransactionResponse:
    def __init__(self, trans_id, type):
        self.trans_id = trans_id
        self.type = type # query or cancel
        self.trans_resp = [] #the array of Sub_Response objects
        self.success = True
        self.err = ""
        pass

    # print Transaction response object
    def __repr__(self):
        print('Trans id: ', str(self.trans_id))
        print('Type: ' , self.type)
        print('Status: ', str(self.success))
        if not self.success:
            print('Error: ', self.err)
            pass
        else:
            for subresponse in self.trans_resp:
                print(subresponse)
                pass
        
        return ''
        
#Response=[]
#contains Order_resp and TransactionResponse
