# ecoding=utf8

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

