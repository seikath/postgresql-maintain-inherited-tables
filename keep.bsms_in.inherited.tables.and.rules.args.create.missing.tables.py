# ecoding=utf8
# 2013-08-11.22.02.43
# dev.add.create.tables.v.1.1
# 2013-08-17.11.54.20
import psycopg2
import psycopg2.extras
import sys
#import datetime
from datetime import timedelta,date
import calendar
import datetime
#import date
import ConfigParser
import os
os.environ['PGCONNECT_TIMEOUT'] = '5'

#################################################
### CONFIG START 
# import local vars - SQL templates
from vars import *
config = ConfigParser.SafeConfigParser()
# get the config at the db.config file in the same directory
config_file=os.path.dirname(os.path.abspath(__file__))+'/'+os.path.basename(os.path.dirname(os.path.abspath(__file__)))+'.cfg'

try:
	config.read(config_file)
	weeks_to_deactivate = config.getint('rule-conf','weeks_to_deactivate')
	weeks_to_activate = config.getint('rule-conf','weeks_to_activate')
	table_name_base = config.get('rule-conf','table_name_base')
	vc_debug = config.getboolean('rule-conf','vc_debug')
	vc_sql_debug = config.getboolean('rule-conf','vc_sql_debug')
	if len(sys.argv) > 1 and sys.argv[1] == 'darkwater':
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Loading db config : db-darkwater-vpn"
		config_db = dict(config.items("db-darkwater-vpn"))
	elif len(sys.argv) > 1 and sys.argv[1] == 'aegir':
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Loading db config : db-aegir-local"
		config_db = dict(config.items("db-aegir-local"))	
	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : [missind db] usege: {0!s} db_server as (aegir|darkwater)".format(sys.argv[0])
		sys.exit(0)	
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] db config : {0!s}".format(config_db)
except ConfigParser.Error, e:
	print "["+str(datetime.now())+"] : ["+sys.argv[1]+"] " + "Error : %s" % (e)
	sys.exit(1)
	
if not vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Debug set to false at " + os.path.abspath(__file__) 
### CONFIG ENDS
#################################################

#diff_week=3
##recent_week=date.isocalendar(datetime.datetime.today())[1]
##print date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=diff_week))
##print date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=diff_week))
#relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=diff_week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=diff_week))))
#relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=diff_week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=diff_week))) + datetime.timedelta(days=+7))
#relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=diff_week))[1])
#relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=diff_week))[0]
#print "relative_first_day_of_week : {0!s}".format(relative_first_day_of_week)
#print "relative_next_first_day_of_week : {0!s}".format(relative_next_first_day_of_week)
#print "relative_week : {0!s}".format(relative_week)
# sys.exit(0)
#print datetime.datetime.weekday(datetime.datetime.today() + datetime.timedelta(weeks=-5))
#print str(datetime.datetime.now()) + "==>" + str(calendar.firstweekday()) + "==>" + str(date.weekday(datetime.datetime.now()+datetime.timedelta))


#print create_missing_inherited_table.format('bsms_in_p'+str(relative_year)+'w'+str(relative_week),relative_first_day_of_week,relative_next_first_day_of_week)
#print create_rule_on_table_inherited.format(str(relative_year)+'w'+str(relative_week),relative_first_day_of_week,relative_next_first_day_of_week)
#sys.exit(0)
### Initiate db link 
try:
    conn_db = psycopg2.connect(**config_db)
    conn_db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
except:
    print "["+str(datetime.now())+"] : ["+sys.argv[1]+"] Not able to connect:: %s !!".format(config_db)
    sys.exit(1)

### Open db cursor
cur_db = conn_db.cursor(cursor_factory=psycopg2.extras.DictCursor)



for week in xrange(-weeks_to_deactivate,-1):
	relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
	relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
	relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
	relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
	rulename='route_rule_bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	#print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] {0!s}".format(week)
	try:
		# 2013-08-10.21.54.14 check rule to deactivate
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking is the rule for {0!s} week is ebabled..".format(week)
		if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(cur_db.mogrify(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename}))
		cur_db.execute(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename})
	except psycopg2.Error, e:
		if vc_debug :
			print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] {0!s} conencting to {1!s} at {2!s} as user {3!s}".
				format(
					e.args[0].rstrip('\r\n'),
					config_db.get('host').rstrip('\r\n'),
					config_db.get('dbname').rstrip('\r\n'),
					config_db.get('user').rstrip('\r\n'),
				)
			)
	rec_deactiavate = cur_db.fetchone()
	
	if int(cur_db.rowcount) > 0:
		# tva e O 
		if rec_deactiavate['active'] == 'O':
			print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have active rule {0!s} to deactivate".format(rec_deactiavate['rulename'])
			sql_to_deactivete="alter table {0!s} disable rule {1!s};".format(table_name_base,rec_deactiavate['rulename'])
			try:
				cur_db.execute(sql_to_deactivete)
				if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] executed '{0!s}'".format(sql_to_deactivete)
			except psycopg2.Error, e:
				#if vc_debug :
				print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR : {0!s} on executing of {1!s}".
					format(
						e.args[0].rstrip('\r\n'),
						sql_to_deactivete.rstrip('\r\n'),
					)
				)
	
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have inactive rule {0!s}".format(rec_deactiavate['rulename'])
	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We dont have rule serving {0!s} weeks".format(week)
	
# Start checking inherited tables
if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Start checking inherited tables {0!s} weeks ahead..".format(weeks_to_activate-1)
for week in xrange(1,weeks_to_activate):
	relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
	relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
	relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
	relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
	rulename='route_rule_bsms_in_p{0!s}w{1!s}'.format(relative_year,relative_week)
	inherited_table = (datetime.datetime.today() + datetime.timedelta(weeks=week)).strftime("bsms_in_p%Yw%U")
	if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking inherited table {0!s} existance..".format(inherited_table)
	if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(cur_db.mogrify(query_table_exists,{'inherited_table' : inherited_table}))
	try:
		cur_db.execute(query_table_exists,{'inherited_table' : inherited_table})
	except psycopg2.Error, e:
		if vc_debug :
			print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR {0!s} executing {1!s}".
				format(
					e.args[0].rstrip('\r\n'),
					query_table_exists
				)
			)
		sys.exit(1)
	if int(cur_db.rowcount) > 0:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {0!s} exists.".format(inherited_table)
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking if table {1!s} inherits {0!s}".format(table_name_base,inherited_table)
		try:
			cur_db.execute(query_table_is_inherited,{'inherited_table' : inherited_table,'table_name_base' : table_name_base})
		except psycopg2.Error, e:
			if vc_debug :
				print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR {0!s} executing {1!s}".
					format(
						e.args[0].rstrip('\r\n'),
						query_table_is_inherited
					)
				)
			sys.exit(1)
		if int(cur_db.rowcount) > 0:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {1!s} inherits {0!s}".format(table_name_base,inherited_table)
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {1!s} does not inherit {0!s}".format(table_name_base,inherited_table)
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Altering table {1!s} to inherit {0!s}".format(table_name_base,inherited_table)
			alter_table_inherit_tmp = alter_table_inherit % (inherited_table,'',table_name_base);
			if vc_sql_debug : print cur_db.mogrify(alter_table_inherit_tmp)
			try:
				cur_db.execute(alter_table_inherit_tmp)
				if vc_sql_debug : print cur_db.mogrify(alter_table_inherit_tmp)
			except psycopg2.Error, e:
				if vc_debug :
					print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR {0!s} executing {1!s}".
						format(
							e.args[0].rstrip('\r\n'),
							query_table_is_inherited
						)
					)
				sys.exit(1)
		# Check corresponsding rule 
		try:
			# 2013-08-10.21.54.14 check rule to activate
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Checking is the rule {1!s} for {0!s} week is ebabled..".format(week,rulename)
			if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(cur_db.mogrify(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename}))
			cur_db.execute(query_rule,{'week' : week,'enabled' : "D",'tablename': table_name_base,'rulename':rulename})
		except psycopg2.Error, e:
			if vc_debug :
				print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] {0!s} conencting to {1!s} at {2!s} as user {3!s}".
					format(
						e.args[0].rstrip('\r\n'),
						config_db.get('host').rstrip('\r\n'),
						config_db.get('dbname').rstrip('\r\n'),
						config_db.get('user').rstrip('\r\n'),
					)
				)
		rec_actiavate = cur_db.fetchone()
		
		if int(cur_db.rowcount) > 0:
			# tva e O 
			if rec_actiavate['active'] == 'D':
				if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have active rule {0!s} to activate".format(rec_actiavate['rulename'])
				sql_to_activete="alter table {0!s} enable rule {1!s};".format(table_name_base,rec_actiavate['rulename'])
				try:
					cur_db.execute(sql_to_activete)
					if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] executed '{0!s}'".format(sql_to_activete)
				except psycopg2.Error, e:
					#if vc_debug :
					print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR : {0!s} on executing of {1!s}".
						format(
							e.args[0].rstrip('\r\n'),
							sql_to_activete.rstrip('\r\n'),
						)
					)
		
			else:
				if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We have active rule {0!s}".format(rec_actiavate['rulename'])
		else:
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] We dont have rule {1!s} serving {0!s} weeks".format(week,rulename)
	else:
		if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] Table {0!s} does NO exist.".format(inherited_table)
		sql_create_missing_table=create_missing_inherited_table % {'tablename':'bsms_in_p'+str(relative_year)+'w'+str(relative_week),'first_day_of_week':relative_first_day_of_week,'next_first_day_of_week':relative_next_first_day_of_week}
		if vc_sql_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] SQL: {0!s}".format(cur_db.mogrify(sql_create_missing_table))
		#relative_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))))
		#relative_next_first_day_of_week=date.isoformat(datetime.datetime.today()+datetime.timedelta(weeks=week)-datetime.timedelta(days=date.weekday(datetime.datetime.today()+datetime.timedelta(weeks=week))) + datetime.timedelta(days=+7))
		#relative_week='{0:02d}'.format(date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[1])
		#relative_year=date.isocalendar(datetime.datetime.today()+datetime.timedelta(weeks=week))[0]
		#print "relative_first_day_of_week : {0!s}".format(relative_first_day_of_week)
		#print "relative_next_first_day_of_week : {0!s}".format(relative_next_first_day_of_week)
		#print "relative_week : {0!s}".format(relative_week)
		#print "relative_year : {0!s}".format(relative_year)
		#if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] To be executed SQL: ".format(create_missing_inherited_table.format(inherited_table,))
		#create_missing_inherited_table.format('bsms_in_p'+str(relative_year)+'w'+str(relative_week),relative_first_day_of_week,relative_next_first_day_of_week)
		try:
			cur_db.execute(sql_create_missing_table)
			if vc_debug : print "["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] created table '{0!s}'".format('bsms_in_p'+str(relative_year)+'w'+str(relative_week))
		except psycopg2.Error, e:
			#if vc_debug :
			print ("["+str(datetime.datetime.now())+"] : ["+sys.argv[1]+"] ERROR : {0!s} on executing of {1!s}".
				format(
					e.args[0].rstrip('\r\n'),
					sql_create_missing_table.rstrip('\r\n'),
				)
			)


cur_db.close()
conn_db.close()
sys.exit (0)


