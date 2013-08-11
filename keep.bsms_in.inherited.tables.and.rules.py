# ecoding=utf8

import psycopg2
import psycopg2.extras
import sys
#import datetime
from datetime import timedelta,date
import datetime
#import date
import ConfigParser
import os
os.environ['PGCONNECT_TIMEOUT'] = '5'

#################################################
### CONFIG START 
config = ConfigParser.SafeConfigParser()
# get the config at the db.config file in the same directory
config_file=os.path.dirname(os.path.abspath(__file__))+'/'+os.path.basename(os.path.dirname(os.path.abspath(__file__)))+'.cfg'

#print config_file

try:
	config.read(config_file)
	config_aegir = dict(config.items("db-aegir-local"))
	weeks_to_deactivate = config.getint('rule-conf','weeks_to_deactivate')
	weeks_to_activate = config.getint('rule-conf','weeks_to_activate')
	table_name_base = config.get('rule-conf','table_name_base')
	vc_debug = config.getboolean('rule-conf','vc_debug')
	vc_sql_debug = config.getboolean('rule-conf','vc_sql_debug')
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : db config : {0!s}".format(config_aegir)
except ConfigParser.Error, e:
	print "["+str(datetime.now())+"] : " + "Error : %s" % (e)
	sys.exit(1)
	
vc_sql_debug = False

try:
    conn_aegir = psycopg2.connect(**config_aegir)
    conn_aegir.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
except:
    print "["+str(datetime.now())+"] : Not able to connect:: %s !!".format(config_aegir)
    sys.exit(1)

# sys.exit (0)
cur_aegir = conn_aegir.cursor(cursor_factory=psycopg2.extras.DictCursor)

query_rule = ("""
SELECT n.nspname AS schemaname, c.relname AS tablename, r.rulename, r.ev_enabled as active,
    pg_get_ruledef(r.oid) AS definition
   FROM pg_rewrite r
   JOIN pg_class c ON c.oid = r.ev_class
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE true 
-- and r.rulename <> '_RETURN'::name
and c.relname = %(tablename)s
and  r.rulename like 'route_rule_bsms_in_p' || extract(year from now()+ interval '%(week)s week') || '%%'
-- and r.ev_enabled <> %(enabled)s 
and r.rulename  in (
	select  
	'route_rule_bsms_in_p' 
	|| extract(year from now()+ interval '%(week)s week') 
	|| 'w' || extract(week from now() + interval '%(week)s week') 
	as  rulename
	)
order by r.rulename 
limit 1
;
""")



if not vc_debug : print "["+str(datetime.datetime.now())+"] : Debug set to false at " + os.path.abspath(__file__) 

if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : SQL: ${0!s}".format(cur_aegir.mogrify(query_rule,{'week' : weeks_to_deactivate,'enabled' : "D",'tablename': table_name_base}))


try:
	# 2013-08-10.21.54.14 check rule to deactivate
	cur_aegir.execute(query_rule,{'week' : weeks_to_deactivate,'enabled' : "D",'tablename': table_name_base})
except psycopg2.Error, e:
	if vc_debug :
		print ("["+str(datetime.datetime.now())+"] : {0!s} conencting to {1!s} at {2!s} as user {3!s}".
			format(
				e.args[0].rstrip('\r\n'),
				config_aegir.get('host').rstrip('\r\n'),
				config_aegir.get('dbname').rstrip('\r\n'),
				config_aegir.get('user').rstrip('\r\n'),
			)
		)

rec_deactiavate = cur_aegir.fetchone()

#if vc_debug : print "rules found : [{0!s}]".format(cur_aegir.rowcount)
#if vc_debug : print "active : [{0!s}]".format(rec_deactiavate['active'])

if int(cur_aegir.rowcount) > 0:
	# tva e O 
	if rec_deactiavate['active'] == 'O':
		print "We have active rule {0!s} to deactivate".format(rec_deactiavate['rulename'])
		sql_to_deactivete="alter table {0!s} disable rule {1!s};".format(table_name_base,rec_deactiavate['rulename'])
		try:
			cur_aegir.execute(sql_to_deactivete)
			if vc_debug : print "["+str(datetime.datetime.now())+"] : executed '{0!s}'".format(sql_to_deactivete)
		except psycopg2.Error, e:
			#if vc_debug :
			print ("["+str(datetime.datetime.now())+"] : ERROR : {0!s} on executing of {1!s}".
				format(
					e.args[0].rstrip('\r\n'),
					sql_to_deactivete.rstrip('\r\n'),
				)
			)

	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : We have inactive rule {0!s}".format(rec_deactiavate['rulename'])
else:
	if vc_debug : print "["+str(datetime.datetime.now())+"] : We dont have rule serving {0!s} weeks".format(weeks_to_deactivate)
	

query_table_is_inherited = ("""

SELECT 
pg_inherits.*
, c.relname AS child
, p.relname AS parent
FROM
    pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid)
    JOIN pg_class as p ON (inhparent=p.oid) 
    where true 
    and p.relname = %(table_name_base)s
    and c.relname = %(inherited_table)s
;
""")
query_table_exists = ("""
select 
	table_name 
	from information_schema.tables 
	where true 
	and table_name=%(inherited_table)s
;
""")

alter_table_inherit = ("""
alter table 
	%s 
	%s inherit
	%s
;
""")

create_trigger_on_table_inherited = ("""
CREATE TRIGGER counters_%s
  AFTER INSERT OR DELETE
  ON %s
  FOR EACH ROW
  EXECUTE PROCEDURE update_counters_bsmsin_trigger();
""")

for week in xrange(1,5):
	inherited_table = (datetime.datetime.today() + datetime.timedelta(weeks=week)).strftime("bsms_in_p%Yw%U")
	if vc_debug : print "["+str(datetime.datetime.now())+"] : Checking inherited table {0!s} existance..".format(inherited_table)
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : SQL: {0!s}".format(cur_aegir.mogrify(query_table_exists,{'inherited_table' : inherited_table}))
	try:
		cur_aegir.execute(query_table_exists,{'inherited_table' : inherited_table})
	except psycopg2.Error, e:
		if vc_debug :
			print ("["+str(datetime.datetime.now())+"] : ERROR {0!s} executing {1!s}".
				format(
					e.args[0].rstrip('\r\n'),
					query_table_exists
				)
			)
		sys.exit(1)
	if int(cur_aegir.rowcount) > 0:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : Table {0!s} exists.".format(inherited_table)
		if vc_debug : print "["+str(datetime.datetime.now())+"] : Checking if table {0!s} inherits {1!s}".format(table_name_base,inherited_table)
		try:
			cur_aegir.execute(query_table_is_inherited,{'inherited_table' : inherited_table,'table_name_base' : table_name_base})
		except psycopg2.Error, e:
			if vc_debug :
				print ("["+str(datetime.datetime.now())+"] : ERROR {0!s} executing {1!s}".
					format(
						e.args[0].rstrip('\r\n'),
						query_table_is_inherited
					)
				)
			sys.exit(1)
		if int(cur_aegir.rowcount) > 0:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : Table {1!s} inherits {0!s}".format(table_name_base,inherited_table)
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : Table {1!s} does not inherit {0!s}".format(table_name_base,inherited_table)
			if vc_debug : print "["+str(datetime.datetime.now())+"] : Altering table {1!s} to inherit {0!s}".format(table_name_base,inherited_table)
			alter_table_inherit_tmp = alter_table_inherit % (inherited_table,'',table_name_base);
			if vc_debug : print cur_aegir.mogrify(alter_table_inherit_tmp)
			try:
				cur_aegir.execute(alter_table_inherit_tmp)
				if vc_sql_debug : print cur_aegir.mogrify(alter_table_inherit_tmp)
			except psycopg2.Error, e:
				if vc_debug :
					print ("["+str(datetime.datetime.now())+"] : ERROR {0!s} executing {1!s}".
						format(
							e.args[0].rstrip('\r\n'),
							query_table_is_inherited
						)
					)
				sys.exit(1)

cur_aegir.close()
conn_aegir.close()
sys.exit (0)


