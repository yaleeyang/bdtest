import urllib.request



def sendRequest(url,data):
    '''
    send post request
    :return:
    '''
    if(len(url)==0 ):
        return

    req = urllib.request.Request( url,data=data)
    urllib.request.urlopen(req)
    print('send successful')
