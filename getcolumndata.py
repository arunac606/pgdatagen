from dbconnection import schemacheck
from custlib import getschema

def getcolumndata(conn):
    params = getschema()
    if schemacheck(conn,params['schema']['schema']):
        cur = conn.cursor()
        tablecondition = ' and c.table_name in('+params['schema']['tables']+')' if len(params['schema']['tables']) > 1 else ""

        print('Fetching column details from database..')
        cur.execute("""select c.table_name,c.column_name,c.ordinal_position,
        (case when c.character_maximum_length is null then 0 when c.data_type='text' then 100 
        else c.character_maximum_length end) as character_maximum_length, c.data_type as data_type
        from information_schema.columns c
        inner join pg_class c1 on c.table_name=c1.relname
        inner join pg_catalog.pg_namespace n on c.table_schema=n.nspname and c1.relnamespace=n.oid
        left join pg_catalog.pg_description d on d.objsubid=c.ordinal_position and d.objoid=c1.oid
        where c.table_schema='"""+params['schema']['schema']
        +"'"+ tablecondition + """ order by c.table_name, c.column_name""")

        data = cur.fetchall()
        cur.close()
        return data
    else:
        exit(0)
        return None

if __name__ == '__main__':
    getcolumndata()