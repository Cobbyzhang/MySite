# !usr/bin/env/ python
# coding:utf8

import logging, time
import db

class Field(dict):
    _count = 0
    def __init__(self, *args,  **kw):
        self.name = kw.get('name',None)
        self.primary_key = kw.get('primary_key',False)
        self.max_length = kw.get('max_length',None)
        self.nullable = kw.get('nullable',False)
        self._default = kw.get('default',None)
        self.insertable = kw.get('insertable',True)
        self.updatable = kw.get('updatable', True)
        self.ddl = kw.get('ddl','')
        self._order = Field._count
        Field._count += 1

    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' %(self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)

class IntegerField(Field):
    '''
    >>> a = IntegerField(name='A')
    >>> print a
    <IntegerField:A,bigint,default(0),UI>
    '''
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField,self).__init__(**kw)

class StringField(Field):
    '''
    >>> a = StringField(max_length=10)
    >>> print a.max_length
    10
    >>> print a._order
    1
    '''
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'text'
        super(StringField,self).__init__(**kw)

class FloatField(Field):
    '''
    >>> a = FloatField()
    '''
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default'] = 0.0
        if not 'ddl' in kw:
            kw['ddl'] = 'real'
        super(FloatField,self).__init__(**kw)

class BooleanField(Field):
    '''
    >>> a = BooleanField()
    ''' 
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'bool'
        super(BooleanField,self).__init__(**kw)

class TextField(Field):
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'text'
        super(TextField,self).__init__(**kw)

class BlobField(Field):
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BlobField,self).__init__(**kw)

class VersionField(Field):
    def __init__(self, name=None):
        super(VersionField, self).__init__(name = name, default=0, ddl='bigint')

_triggers = frozenset(['pre_insert','pre_update','pre_delete'])

class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        pass

class Model(dict):
    __metaclass__ = ModelMetaclass
    def __init__(self, **kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'dict' object has no atrribute '%s'"%key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, pk):
        pass

    @classmethod
    def find_first(cls, where, *args):
        pass

    @classmethod
    def find_all(cls, where, *args):
        pass

    @classmethod
    def find_by(cls, where, *args):
        pass

    @classmethod
    def count_all(cls):
        pass

    @classmethod
    def count_by(cls, where, *args):
        pass

    def update(self):
        pass

    def delete(self):
        pass

    def insert(self):
        pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    db.create_engine()
    db.update('drop table if exists user')
    db.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()






