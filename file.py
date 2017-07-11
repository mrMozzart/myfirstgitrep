import sqlite3
import random

def connect(my_db_name):
    try:
        conn = sqlite3.connect('./'+my_db_name)
        curs = conn.cursor()
        print 'Connection to %s success!' %my_db_name
        return conn, curs
    except sqlite3.OperationalError, err:
        print err


def close(my_base, db_name):
    try:
        my_base.close
        print 'Connection to %s closed success!' %db_name
    except sqlite3.OperationalError, err:
        print err

def commit(my_base, my_base_name):
    try:
        my_base.commit()
        print '%s updated!'%my_base_name
    except sqlite3.OperationalError, err:
        print err

def get_query(curs, query, *args):
    try:
        # print query%args
        result = []
        res = curs.execute(query%args)
        for row in res:
            # print row
            result.append(row)
        return result

    except sqlite3.OperationalError, err:
        print err

def data_generate_dict(my_count):
    rand_list= []
    for i in range(my_count):
        rand_list.append(random.randint(1, 50))
    res = {tx: tx ** 2 for tx in rand_list}
    return res

def create_tabs(my_curs, queries, tab_name, columns, my_count):
    get_query(my_curs, queries['create'], tab_name, columns)
    for i in data_generate_dict(my_count).items():
        get_query(my_curs, queries['insert'], tab_name, i)

queries = {'create': 'CREATE TABLE %s (%s)',
           'insert': 'INSERT INTO %s VALUES %s',
           'select': 'SELECT %s FROM %s WHERE %s'}
db_name = 'my_db.db'
tabs = [{'count': 25, 'name': 'tab1', 'columns': 'a int, b int'},
        {'count': 15, 'name': 'tab2','columns': 'a int, b int'}]

my_base, my_curs = connect(db_name)

for tab in tabs:
    create_tabs(my_curs, queries, tab['name'], tab['columns'], tab['count'])

commit(my_base, db_name)

alter = get_query(my_curs, queries['select'], 'a, b', 'tab1','tab1.a || tab1.b not in (SELECT a || b FROM tab2)')
for i in alter:
    get_query(my_curs, queries['insert'], 'tab2', i)
commit(my_base, db_name)

close(my_base, db_name)
