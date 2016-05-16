import time

import requests
from elasticsearch import Elasticsearch

es = Elasticsearch()

starttime = time.time()

usersindexed = 0
starttime = time.time()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


def indexUser(user):
    global usersindexed
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
        print(bcolors.OKBLUE + "Indexed "+user['username'])
        usersindexed += 1



def GetFollowers(username, page=0):
    print(bcolors.OKGREEN + "Retrieving followers list page " + str(page) + " for user " + username)
    try:
        r = requests.get('https://api.scratch.mit.edu/users/{0}/following?offset={1}'.format(username, page * 20))
        status = r.status_code
    except:
        print(bcolors.HEADER + "Rate limited!")
        time.sleep(0.1)
        status = 0

    while status != 200:
        print(bcolors.HEADER + "Rate limited!")
        time.sleep(0.1)
        r = requests.get('https://api.scratch.mit.edu/users/{0}/following?offset={1}'.format(username, page * 20))
        status = r.status_code
    followers = r.json()
    return followers

def IndexFollowers(username):
    page = 0
    followers = GetFollowers(username, page)
    global usersindexed
    while followers:
        for follower in followers:
            indexUser(follower)
        page += 1
        followers = GetFollowers(username, page)


while True:
    res = es.search(index="scratch", body={"query": { "match" : { "indexed" : "0" } }})
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        doc = {
            "doc": {
                "indexed": 1
            }
        }
        es.update(index="scratch", doc_type='user', id=hit["_id"], body=doc)
        IndexFollowers(hit["_source"]['username'])
        print(bcolors.ENDC + "Users indexed per second: "+str(usersindexed/(time.time()-starttime)))
        time.sleep(0.065)
