from dbconnection import connectdb, closedb
from getcolumndata import getcolumndata
from datagen import datagen
from custlib import getschema

def autogenerate():
    conn = connectdb()
    data = getcolumndata(conn)
    params = getschema()
    schema = params['schema']['schema']
    tc = int(params['schema']['testcasecount'])
    print('Creating sql & xml files..')
    [datagen(data,schema,i) for i in range(0,tc)]
    print(f'Sample data created for {tc} test cases.')
    closedb(conn)

if __name__ == '__main__':
    autogenerate()