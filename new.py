import requests
import time
from elasticsearch import Elasticsearch

es = Elasticsearch()
target = open('lastpage', 'w')


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


def indexUser(user):
    userdoc = {
        "username": user['username'],
        "joined": user['history']['joined'],
        "avatar": user['profile']['avatar'],
        "status": user['profile']['status'],
        "bio": user['profile']['bio'],
        "country": user['profile']['country'],
        'indexed': "0"
    }
    res = es.index(index="scratch", doc_type='user', id=user['id'], body=userdoc)
    if res['created']:
        print(bcolors.HEADER + "Indexed "+user['username'])


def GetURL(url):
    try:
        r = requests.get(url)
        status = r.status_code
    except:
        time.sleep(0.1)
        status = 0

    while status != 200:
        time.sleep(0.1)
        r = requests.get(url)
        status = r.status_code
    followers = r.json()
    return followers

page = 0

while True:
    print(bcolors.OKGREEN + 'Fetching page '+str(page))
    users = GetURL('https://scratch.mit.edu/api/v1/user/?offset={0}&limit=20&format=json'.format(page*20))
    target.truncate()
    target.write(str(page))
    for user in users['objects']:
        userobject = GetURL('https://api-staging.scratch.mit.edu/users/{0}'.format(user['username']))
        time.sleep(0.065)
        indexUser(userobject)
    page+=1

