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
# port – connection port number (defaults to 5432 if not provided)

import psycopg2
import sys
from datetime import datetime

# CONFIGs, SQL etc ========= [ start ] 
# keyword arguments:
config_aegir = {
  'user': 'pdns',
  'password': 'pdns_user_passwd',
  'host': '172.16.0.1',
  'dbname': 'nova',
  'port': '6543',
}
# dns



config_aegir = {
  'user': 'pdns',
  'password': 'pdns_user_passwd',
  'host': '172.16.0.1',
  'dbname': 'nova',
  'port': '6543',
}


sys.exit (0)

config_darkwater = {
  'user': 'pdns_user',
  'password': 'pdns_user_passwd',
  'host': '127.0.0.1',
  'dbname': 'pdns',
  'port': '6543',

}

query = ("""select
i.id
, i.hostname
-- , i.host
-- , i.vm_state
-- , m.uuid
-- , f.id
-- , lower(f.address) as fixed_ip
, lower(s.address) as floating_ip
from
instances i  left join instance_id_mappings m on i.id=m.id
left join fixed_ips f on m.uuid=f.instance_uuid
left join floating_ips s on f.id=s.fixed_ip_id
where true
-- and i.vm_state != 'deleted'
and i.host is not null""")

query_darkwater = ("""
delete from records where content = 'None';

""")
debug = False
#debug = True
vc_debug = False
#vc_debug = True

if not vc_debug : print "["+str(datetime.now())+"] : Debug set to false at /home/epg/bin/epg-pdns-03/update.pdns.v.0.3.py"

# CONFIGs, SQL etc ========= [ end ] 

# initialize db conection to nova
try:
	cnx_aegir = MySQLdb.connect(**config_nova)
	# open nova cursor
	cursor_nova = cnx_nova.cursor()
except MySQLdb.Error, e:
	print "["+str(datetime.now())+"] : " + "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit (1)
	
# start the data processing
try:
	cursor_nova.execute(query)
	# initialize db conection to pdns
	try:
		cnx_pdns = MySQLdb.connect(**config_pdns)
		# open pdns cursor
		cursor_pdns = cnx_pdns.cursor()
	except MySQLdb.Error, e:
		print "["+str(datetime.now())+"] : " + "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit (1)
except Exception, e:
	print "["+str(datetime.now())+"] : " + repr(e)
	sys.exit (1)
