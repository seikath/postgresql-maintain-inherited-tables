# ecoding=utf8
# http://eventlet.net/


urls = ["http://127.0.0.1:13000/status.txt?user=seik&password=seik&smsc=first",
     "http://10.9.0.5:13000/status.txt?user=seik&password=seik&smsc=second",
     "http://10.9.0.157:13000/status.txt?user=seik&password=seik&smsc=third"]

import eventlet
from eventlet.green import urllib2

def fetch(url):

  return urllib2.urlopen(url).read()

pool = eventlet.GreenPool()

for body in pool.imap(fetch, urls):
  print "got body", len(body)


#NAME
    #eventlet

#FILE
    #/usr/lib/python2.6/site-packages/eventlet-0.13.0-py2.6.egg/eventlet/__init__.py

#PACKAGE CONTENTS
    #api
    #backdoor
    #convenience
    #corolocal
    #coros
    #db_pool
    #debug
    #event
    #green (package)
    #greenio
    #greenpool
    #greenthread
    #hubs (package)
    #patcher
    #pool
    #pools
    #proc
    #processes
    #queue
    #saranwrap
    #semaphore
    #support (package)
    #timeout
    #tpool
    #twistedutil (package)
    #util
    #websocket
    #wsgi

#FUNCTIONS
    #getcurrent(...)

#DATA
    #__version__ = '0.13.0'
    #version_info = (0, 13, 0)

#VERSION
    #0.13.0
