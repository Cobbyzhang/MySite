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

    def __repr__(self):
        return self.name

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
    >>> a.default
    0.0
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

def _gen_sql(table_name, mappings):
    pk = None
    sql = ['-- generating SQL for %s:' % table_name, 'create table `%s` (' % table_name]
    for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % n)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and '  `%s` %s,' % (f.name, ddl) or '  `%s` %s not null,' % (f.name, ddl))
    sql.append('  primary key(`%s`)' % pk)
    sql.append(');')
    return '\n'.join(sql)

class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        if name == 'Model':
            return type.__new__(cls,name,bases,attrs)
        if not hasattr(cls,'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s'%name)
        mappings = dict()
        primary_key = None
        for k,v in attrs.iteritems():
            if isinstance(v,Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping:%s->%s' %(k,v))
                if v.primary_key:
                    if primary_key is not None:
                        raise TypeError('No more than 1 primary key in class:%s!')% name
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable')
                        v.nullable = False
                    primary_key = v
                mappings[k] = v
        if not primary_key.primary_key:
            raise TypeError('Primary key not defined in class: %s' %name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__']= name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs['__sql__'] = _gen_sql(attrs['__table__'],mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None
        return type.__new__(cls,name,bases,attrs)

class Model(dict):
    '''
    >>> class User(Model):
    ...     id_number = IntegerField(primary_key=True)
    ...     name = StringField()
    ...     email = StringField(updatable = False)
    ...     passwd = StringField(default = '123456')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.last_modified = time.time()
    >>> u = User(id_number=101,name='Cobby',email = 'ycz0098@mail.ustc.edu.cn')
    >>> u.insert()
    1L
    >>> u.insert()
    0L
    >>> u.__class__.get(101).id_number
    101L
    >>> u.email
    'ycz0098@mail.ustc.edu.cn'
    >>> r = User.get(101)
    >>> r.name
    'Cobby'
    >>> r.name = 'Zhang'
    >>> r.update()
    1L
    >>> r.delete()
    1L
    >>> r.delete()
    0L
    >>> print User.get(101)
    None
    '''
    __metaclass__ = ModelMetaclass
    def __init__(self, **kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'dict' object has no atrribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, pk):
        ins = db.select_one('select * from %s where %s=?'%(cls.__table__,cls.__primary_key__.name),str(pk))
        return cls(**ins) if ins else None 
   
    @classmethod
    def find_first(cls, where, *args):
        d = db.select_one('select * from %s %s' % (cls.__table__,where), *args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls, where, *args):
        L = db.select('select * from `%s` '% clc.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, *args):
        L = db.select('select * from `%s` %s'%(cls.__table__, where),*args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        return db.select_int('select count(%s) from `%s`'% (cls.__primary_key__.name,cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        return db.select_int('select count(%s) from %s %s'%(cls.__primary_key__, cls.__table__, where), *args)

    def update(self):
        self.pre_update and self.pre_update()
        ins = self.__class__.get(self[self.__primary_key__.name])
        if not ins:
            return self.insert()
        L = []
        args = []
        for k,v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self,k):
                    args.append(getattr(self,k))
                    L.append('%s=?' % k)
        pk = self.__primary_key__.name
        args.append(getattr(self,pk))
        sql = 'update %s set %s where %s=?'%(self.__table__,','.join(L),pk) 
        return db.update(sql,*args)

    def delete(self):
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        return db.update('delete from %s where %s=? '%(self.__table__,pk),self[pk])

    def insert(self):
        self.pre_insert and self.pre_insert()
        pk = self.__primary_key__.name
        ins = self.__class__.get(self[pk]) if hasattr(self,pk) else None
        if not ins:
            for k,v in self.__mappings__.iteritems():
                if v.insertable:
                    if not hasattr(self,k):
                        setattr(self,k,v.default)
                else:
                    if k in self.keys():
                        self.pop(k)
                        logging.info('You couldn\'t insert key %s'%k)
            return db.insert(self.__table__, **self)
        logging.info('Insert Error')
        return 0L

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    db.create_engine()
    db.update('drop table if exists user')
    db.update('create table user (id_number int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()






