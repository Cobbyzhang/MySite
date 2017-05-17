# !/usr/bin/env python
# coding:utf8

import  threading, logging, functools

class _Engine(object):
    def __init__(self,connect):
        self._connect = connect
    def connect(self):
        return self._connect()

class DBerror(Exception):
    pass

class MultiColumnsError(DBerror):
    pass

class _LazyConnection(object):
    def __init__(self):
        self.connection = None
    def cursor(self):
        if self.connection is None:
            self.connection = engine.connect()
            logging.info('Open connection <%s>... '%hex(id(connection)))
        return self.connection.cursor()
    def commit(self):
        self.connection.commit()
    def rollback(self):
        self.connection.rollback()
    def cleanup(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info('close connection <%s>'%hex(id(connection)))

engine = None

class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transaction = 0
    def is_init(self):
        return not self.connection is None
    def init(self):
        self.connection = _LazyConnection()
        logging.info('open lazy_connection...')
        self.transaction = 0
    def cleanup(self):
        self.connection.cleanup()
        self.connection = None
    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()

class Dict(dict):
    '''
    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d1.y = 200
    >>> d1['y']
    200
    >>> d2 = Dict(a=1, b=2, c='3')
    >>> d2.c
    '3'
    >>> d2['a']
    1
    >>> d2['d']
    Traceback (most recent call last):
    ...
    KeyError: 'd'
    >>> d2.d
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'd'
    '''
    def __init__(self,names=(),values=(),**kw):
        super(Dict,self).__init__(**kw)
        for k,v in zip(names,values):
            self[k] = v
    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)
    def __setattr__(self,key,value):
        self[key] = value

class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

def with_connection(func):
    functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper


class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transaction = _db_ctx.transaction + 1
        logging.info('begin transaction...'if _db_ctx.transaction==1 else 'join current transaction')
        return self
    def __exit(self):
        global _db_ctx
        _db_ctx.transaction = _db_ctx.transaction - 1
        try:
            if _db_ctx.transaction==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()
    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. Try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok')
            raise
    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()    
        logging.info('rollback ok.')

def transaction():
    return _TransactionCtx()

def with_transaction(func):
    @functools.wrap(func)
    def _wrapper(*arg, **kw):
        with _TransactionCtx():
            return func(*arg, **kw)
    return _wrapper

def create_engine(user='Cobby', passward='', db='learn_python_webapp', host='127.0.0.1', port=3306):
    global engine
    import MySQLdb
    if engine is not None:
        raise DBerror('The engine has been created')
    engine = _Engine(lambda: MySQLdb.connect(user=user, passwd=passward, db=db, host=host, port=port))
    logging.info('Init mysql engine <%s> ok' %hex(id(engine)))

def _select(sql, first, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL:%s, ARGS:%s'%(sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names,values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql, *args):
    return _select(sql,True, *args)

@with_connection
def select_int(sql, *args):
    d = _select(sql, True, *args)
    if len(d)!=1:
        raise MultiColumnsError
    return d.values()[0]

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('SQL:%s, ARGS:%s'%(sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        r = cursor.rowcount
        if _db_ctx.transaction == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def insert(table, **kw):
    '''
    >>> u1 = dict(id=100, name='Cobby',age=24)
    >>> insert('student_test',**u1)
    1L
    >>> u2 = dict(id=101,name='Zhang',age=24)
    >>> insert('student_test',**u2)
    1L
    '''
    cols,args = zip(*kw.iteritems())
    sql = 'insert into `%s` (%s) values (%s) '%(table,','.join(['`%s`'%col for col in cols]),','.join(['?'for i in range(len(cols))]))
    return _update(sql, *args)

def update(sql, *args):
    '''
    >>> u1 = dict(id=200, name='Cobby',age=24)
    >>> u2 = dict(id=201,name='Zhang',age=24)
    >>> insert('student_test',**u1)
    1L
    >>> insert('student_test',**u2)
    1L
    >>> u3 = select_one('select * from student_test where id=?',100)
    >>> u3.age
    24L
    >>> update('update student_test set name=? where id=?','Cobby','201')
    1L
    '''
    return _update(sql, *args)


if __name__ =='__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine()
    update('drop table if exists student_test')
    update('create table student_test (id int primary key, name text, age int)')
    import doctest
    doctest.testmod()   
