import sys
import time
import os
import platform 
import subprocess
import pymongo
import config
from bson.son import SON
from socket import socket
from pymongo import Connection

CARBON_SERVER = config.CARBON_SERVER
CARBON_PORT = config.CARBON_PORT
GRAPHITE_PREFIX = config.GRAPHITE_PREFIX
MONGO_HOST = config.MONGO_HOST
MONGO_PORT = config.MONGO_PORT
MONGO_USER = config.MONGO_USER
MONGO_PWD = config.MONGO_PWD

class profiler( object ):
  def __init__( self ):
    self.conn = Connection( MONGO_HOST, MONGO_PORT )
    db = getattr( self.conn, "admin" )
    db.authenticate( MONGO_USER, MONGO_PWD )

  def get_profiler_data( self ):
      
    # get profiler data
    db = getattr( self.conn, "test" )
  
    #
    # kg fixme, need todo rolling window vs totals
    #

    out = db.system.profile.aggregate([
            {"$group": 
              { "_id" :"$op",
                "count":{"$sum":1},
                "max response time":{"$max":"$millis"},
                "avg response time":{"$avg":"$millis"}
              }
            }
      ])
  
    return out['result']

  def put_graphite( self, data, thedate ):

    self.sock = socket()
    self.sock.connect( (CARBON_SERVER,CARBON_PORT) )

    lines = []
    metrics = [['max response time','max'],['avg response time','avg'],['count','count']]

    for i in data:
      for m in metrics:
        str = "%s.%s.%s %s %s" % ( GRAPHITE_PREFIX, i['_id'], m[1], i[m[0]], thedate)
        lines.append(str)
  
    message = '\n'.join(lines) + '\n' #all lines must end in a newline
    print "sending message\n"
    print '-' * 80
    print message
    print
    
    self.sock.sendall(message)

  def test_sock(self):

    self.sock = socket()
    try:
      self.sock.connect( (CARBON_SERVER,CARBON_PORT) )
      return True
    except:
      print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PORT }
      sys.exit(1)

if __name__ == "__main__":

  delay = 60 
  if len(sys.argv) > 1:
    delay = int( sys.argv[1] )

  p = profiler()

  if p.test_sock():

    while True:

      now = int( time.time() )
      thedate = now

      out = p.put_graphite( p.get_profiler_data(), thedate )

      time.sleep(delay)

