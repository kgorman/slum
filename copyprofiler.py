import sys
import time
import os
import platform 
import subprocess
import pymongo
import config
import datetime
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
MONGO_DB = config.MONGO_DB

class profiler( object ):
  def __init__( self ):
    self.conn = Connection( MONGO_HOST, MONGO_PORT )
    db = getattr( self.conn, "admin" )
    db.authenticate( MONGO_USER, MONGO_PWD )

  def get_profiler_data( self ):
      
    # get profiler data
    db = getattr( self.conn, MONGO_DB )
  
    out = db.system.profile.find()

    for i in out:
      print i
      db.kgprofiler.save(i)
    return out

if __name__ == "__main__":

  p = profiler()
  p.get_profiler_data()