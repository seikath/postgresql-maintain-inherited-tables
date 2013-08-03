# coding: utf-8
# 2013-07-27.00.46.56
############################################################################################
# cd /opt/proceed.bsms/psycopg2-2.5.1-1
# git remote add origin ssh://git@bitbucket.org/seikath/aegir.bsms.psycopg2-2.5.1-1.git
# git push -u origin --all   # to push up the repo for the first time
############################################################################################
# OR start from scratch
# mkdir /path/to/your/project
# cd /path/to/your/project
# git init
# git remote add origin ssh://git@bitbucket.org/seikath/aegir.bsms.psycopg2-2.5.1-1.git
# git add *
# git commit -m "psycopg2-2.5.1-1.git"
############################################################################################
# root@aegir.voicecom.bg:[Sat Jul 27 02:02:04]:[/opt/proceed.bsms/psycopg2-2.5.1-1]$ git push -u origin --all
# Counting objects: 3, done.
# Delta compression using up to 2 threads.
# Compressing objects: 100% (2/2), done.
# Writing objects: 100% (3/3), 439 bytes, done.
# Total 3 (delta 0), reused 0 (delta 0)
# To ssh://git@bitbucket.org/seikath/aegir.bsms.psycopg2-2.5.1-1.git
# * [new branch]      master -> master
# Branch master set up to track remote branch master from origin.
############################################################################################
## git checkout -b lean_db_conection
## git push origin lean_db_conection
#  https://drupal.org/node/1066342
# http://initd.org/psycopg/docs/module.html#exceptions
# http://initd.org/psycopg/docs/usage.html
# The basic connection parameters are:
# dbname – the database name (only in the dsn string)
# database – the database name (only as keyword argument)
# user – user name used to authenticate
# password – password used to authenticate
# host – database host address (defaults to UNIX socket if not provided)
# port – connection port number (defaults to 5432 if not provided
# SyntaxError: Non-ASCII character '\xe2' in file db.pgsql.inc.py on line 31, but no encoding declared; see http://www.python.org/peps/pep-0263.html for details
# http://mohsinpage.wordpress.com/2010/06/10/python-syntaxerror-non-ascii-character-xe2-in-file/

#The connection parameters can be specified either as a libpq connection string using the dsn parameter:
#conn = psycopg2.connect("dbname=test user=postgres password=secret")
	
#or using a set of keyword arguments:
#conn = psycopg2.connect(database="test", user="postgres", password="secret")
# http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS

import psycopg2
import sys
import string
from datetime import datetime

# CONFIGs, SQL etc ========= [ start ] 
# keyword arguments:
config_aegir = {
  'user': 'seik',
  'password': 'pdns_user_passwd',
#  'host': '172.16.0.1',
  'host': '127.0.0.1',
  'dbname': 'vc_stats',
  'port': '6543',
  'connect_timeout' : '2',
}
# dns

#print "["+str(datetime.now())+"] : " + "Status : conencting to {0} at {1} as user {2}".format(config_aegir.get('dbname'),config_aegir.get('host'),config_aegir.get('user'))
#sys.exit (0)
#print config_aegir.get('dbname')
#print config_aegir.get('port')
#print config_aegir.get('connect_timeout')
#print ("["+str(datetime.now())+"] : " + "Status : conencting to {} at {} as user {}".format(config_aegir.get('host'),config_aegir.get('dbname'),config_aegir.get('user')))
##>>> '{0}, {1}, {2}'.format('a', 'b', 'c')
##'a, b, c'
##>>> '{}, {}, {}'.format('a', 'b', 'c')  # 2.7+ only
# sys.exit (0)
#config_aegir = {
  #'user': 'pdns',
  #'password': 'pdns_user_passwd',
  #'host': '172.16.0.1',
  #'dbname': 'nova',
  #'port': '6543',
#}

# initialize db conection to nova

try:
	cnx_aegir = psycopg2.connect(**config_aegir)
	# open nova cursor
	cursor_aegir = cnx_aegir.cursor()
except psycopg2.Error, e:
	print ("["+str(datetime.now())+"] : {0!s} conencting to {1!s} at {2!s} as user {3!s}".
	format(
		e.args[0].rstrip('\r\n'),
		config_aegir.get('host').rstrip('\r\n'),
		config_aegir.get('dbname').rstrip('\r\n'),
		config_aegir.get('user').rstrip('\r\n'),
	)
	)
	sys.exit (1)
	
sys.exit (0)
