# -*- coding: utf-8 -*-
import riak
import argparse
import socket
import logging

RIAK_CLUSTER = {
                "n1":["192.168.56.223",8098],
                }

socket.setdefaulttimeout(1.0)

##############
# ManageRiak #
##############
class ManageRiak():
  """ Class ManageRiak : for manage Riak NoSql Db """

  rc = ""


##############
# Constructor
##############
  def __init__(self,myLog):
    self._log = myLog


###################
# _connectNode
###################
  def _connectNode(self,node):
    try:
      self.rc = riak.RiakClient(host=RIAK_CLUSTER["%s"%node][0],port=RIAK_CLUSTER["%s"%node][1])
      if self.rc.is_alive():
        return 1
      else:
        return 0
    except Exception,e:
      print e
      return 0
    except socket.timeout,f:
      print f
      return 0


##################
# connect
##################
  def connect(self):
    """ connect to Riak

    This method is created to connect over a
    Riak cluster
    """
    for x,y in RIAK_CLUSTER.iteritems():
      if self._connectNode(x):
        return 1
    return 0

##############
# getClient
##############
  def getClient(self):
    return self.rc

def main():
  myLogger = logging.getLogger("Manage Riak")
  mr = ManageRiak(myLogger)
  print mr

if __name__ == "__main__":
  main()
