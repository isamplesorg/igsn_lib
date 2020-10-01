'''

'''

import json

CREDENTIALS_PATH = "/Volumes/Keybase/private/davev/credentials.json"

def getCredentials(account_id):
    data = json.load(open(CREDENTIALS_PATH, 'r'))
    return data[account_id]
