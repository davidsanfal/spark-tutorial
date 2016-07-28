# encoding=utf8
import requests
import time
import json
import os
import sys
import socket


reload(sys)
sys.setdefaultencoding('utf8')


def write_info(timestamp, data):
    with open('..\\data\\data{}.json'.format(timestamp), 'w+') as f:
        data = "\n".join([json.dumps({"id": d["id"],
                                      "pokemonId": d['pokemonId'],
                                      "longitude": d["longitude"],
                                      "latitude": d["latitude"],
                                      "readed": timestamp}) for d in data])
        f.write(data)


def create_pokedex():
    pokemons = None
    with open('..\\data\\pokemons.json', 'r+') as f:
        pokemons = f.read()
    pokemons = json.loads(pokemons).get('pokemon')
    pokedex = "\n".join(["{},{}".format(p["id"], p["name"]) for p in pokemons])
    with open('..\\data\\pokedex.txt', 'w+') as f:
        f.write(pokedex)


def read_pokevision(lat=40.4499529, lon=-3.6897580999999993):
    data = requests.get('https://pokevision.com/map/data/{}/{}'.format(lat, lon)).json()["pokemon"]
    timestamp = int(time.time())
    return ["{},{},{},{},{}\n".format(d["id"], d['pokemonId'], d["latitude"], d["longitude"], timestamp) for d in data]


def server(host='localhost', port=8888):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((host, port))
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    s.listen(1)
    print "Listening ..."
    conn, addr = s.accept()
    print "Accepted"
    while 1:
        time.sleep(0.5)
        conn.sendall(''.join(read_pokevision()))
    conn.close()
    s.close()

if __name__ == '__main__':
    server()
