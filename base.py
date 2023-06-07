
from .column import Column
from .primary_key import PrimaryKey
import copy
import json
from typing import List, Dict
import tablestore
from .filter import Filter
from .client import client

_client_key = '_tb_client'

# https://www.liaoxuefeng.com/wiki/1016959663602400/1018490605531840

class MetaClass(type):
    def __init__(cls, classname, bases, attrs, **kw):
        # early-consume registry from the initial declarative base,
        # assign privately to not conflict with subclass attributes named
        # "registry"
        if classname=='BaseMode':
            return type.__init__(cls, classname, bases, attrs)
        
        cls.__columns__ = []
        cls.__primaryKeys__ = []
        cls.__secondaryIndexKeyMap__ = {}

        if "__table__" not in attrs and "__tablename__" not in attrs:
            raise TypeError("<%s> does not have a __table__ or __tablename__" % (classname))
        cls.__table__ = attrs['__table__']
        if not cls.__table__:
            cls.__table__ = attrs['__tablename__']

        for k,v in attrs.items():
            if isinstance(v, Column):
                cls.__columns__.append(k)
            elif isinstance(v, PrimaryKey):
                cls.__primaryKeys__.insert(v.index, k)
                v.name = k

        for index in range(1, len(cls.__primaryKeys__)):
            copy_keys = copy.copy(cls.__primaryKeys__)
            pk = copy_keys.pop(index)
            copy_keys.insert(0, pk)
            cls.__secondaryIndexKeyMap__[pk] = copy_keys
        cls.__secondaryIndexKeyMap__[cls.__primaryKeys__[0]] = cls.__primaryKeys__

class BaseMode(dict, metaclass=MetaClass):

    def __init__(self, **kwargs) -> None:
        cls_ = type(self)

        for k in kwargs:
            if not hasattr(cls_, k):
                raise TypeError(
                    "%r is an invalid keyword argument for %s" % (k, cls_.__name__)
                )
            setattr(self, k, kwargs[k])

    def __getattr__(self, key):
        return self.get(key)

    def __setattr__(self, key, value):
        _value = value
        att = self.__class__.__dict__.get(key)
        if isinstance(att, Column) and att.json_obj:
            if isinstance(value, str):
                _value = json.loads(value)
        self[key] = _value

    def _get_pk_value_list(self, main_pk_name = '', default=tablestore.INF_MIN) -> List:
        """返回所有pk的kv列表，顺序以main_pk_name为第一个，其余按照主键顺序排列

        Args:
            main_pk_name (str, optional): _description_. Defaults to ''.
            default (_type_, optional): 如果值为空时使用的默认值. Defaults to tablestore.INF_MIN.

        Returns:
            List: [(main_pk_name, value),(key_name, value),(key_name, value)]
        """
        pk_name_list = self.__class__.__primaryKeys__
        if main_pk_name:
            pk_name_list = self.__class__.__secondaryIndexKeyMap__[main_pk_name]
        pk_values = []
        for key in pk_name_list:
            value = self.get(key)
            if not value:
                if self.__class__.__dict__[key].autoincrement:
                    value = tablestore.PK_AUTO_INCR
                else:
                    value = default
            pk_values.append( (key, value ) )
        return pk_values

    # 插入或覆盖更新数据
    @client
    def save(self, return_type = tablestore.ReturnType.RT_PK, **kwargs):
        client = kwargs.get(_client_key)
        if not client:
            raise ValueError('not client')
        primary_key = self._get_pk_value_list()

        attribute_columns = []
        for key in self.__class__.__columns__:
            value = getattr(self, key)
            if isinstance(value, list) or isinstance(value, dict):
                value = json.dumps(value, ensure_ascii=False).encode('utf8')
            attribute_columns.append((key, value))
        row = tablestore.Row(primary_key, attribute_columns)
        consumed, return_row = client.put_row(self.__class__.__table__, row, return_type = return_type)
        return consumed, return_row

    @client
    def update_not_empty(self, **kwargs):
        client = kwargs.get(_client_key)
        if not client:
            raise ValueError('not client')
        primary_key = self._get_pk_value_list()

        attribute_columns = []

        for key in self.__class__.__columns__:
            value = getattr(self, key)
            if value:
                if isinstance(value, list) or isinstance(value, dict):
                    value = json.dumps(value, ensure_ascii=False).encode('utf8')
                attribute_columns.append((key, value))

        update_of_attribute_columns = {
            'PUT' : attribute_columns
        }
        row = tablestore.Row(primary_key, update_of_attribute_columns)
        return client.update_row(self.__class__.__table__, row, None)

    @classmethod
    @client
    def find_by_pk(cls, pk_values: List, **kwargs):
        """使用完整的主键查询一条数据

        Args:
            pk_values (List): 完整的主键值，按主键顺序排列

        Raises:
            ValueError: 没有client或者主键值数量不对应主键数量

        Returns:
            Self@BaseMode: model
        """
        client = kwargs.get(_client_key)
        if not client:
            raise ValueError('not client')
        pk_list = cls.__primaryKeys__
        if len(pk_list) != len(pk_values):
            raise ValueError('pk_values length not equal pk nums')
        
        primary_key = []
        for index, k  in enumerate(pk_list):
            primary_key.append( (k, pk_values[index]) )

        _, row, _ = client.get_row(cls.__table__, primary_key)
        if not row:
            return None
        model = cls()
        for column_name, value, _ in row.attribute_columns:
            setattr(model, column_name, value)
        return model
    
    # 范围查找，当前一个PK为范围而不是具体某个值时，后面的PK无效
    @classmethod
    @client
    def query_by_pk(cls, pk_filters: List[Filter], column_filters: List[Filter]=[], limit = None, **kwargs):
        client = kwargs.get(_client_key)
        if not client:
            raise ValueError('not client')

        start_primary_key = []
        end_primary_key = []

        main_pk: PrimaryKey = pk_filters[0].column
        pk_name_list = cls.__secondaryIndexKeyMap__[main_pk.name]
        for pk_name in pk_name_list:
            for filter in pk_filters:
                found = False
                if filter.column.name == pk_name:
                    start_primary_key.append(filter.operator[0])
                    end_primary_key.append(filter.operator[1])
                    found = True
                    break
                if not found:
                    start_primary_key.append((pk_name, tablestore.INF_MIN))
                    end_primary_key.append((pk_name, tablestore.INF_MAX))

        column_filter  = None
        conditions = []
        for c_filter in column_filters:
            conditions.extend(c_filter.operator)

        if len(conditions) == 1:
            column_filter = conditions[0]
        elif len(conditions) > 1:
            column_filter = tablestore.CompositeColumnCondition(tablestore.LogicalOperator.AND)
            for condition in conditions:
                column_filter.add_sub_condition(condition)
        
        consumed, next_start_primary_key, rows, next_token = client.get_range(
            main_pk.sce_index_tb_name, tablestore.Direction.FORWARD, start_primary_key, end_primary_key, columns_to_get=cls.__columns__, limit=limit, column_filter=column_filter)

        all_rows = []
        all_rows.extend(rows)

        while next_start_primary_key is not None:
            consumed, next_start_primary_key, rows, next_token = client.get_range(
            main_pk.sce_index_tb_name, tablestore.Direction.FORWARD, next_start_primary_key, end_primary_key, columns_to_get=cls.__columns__, limit=limit, column_filter=column_filter)
            all_rows.extend(rows)

        results = []
        for row in all_rows:
                result = {col[0]: col[1] for col in row.attribute_columns}
                result.update({col[0]: col[1] for col in row.primary_key})
                # result['aid'], result['id'], result['image_id'] = [col[1] for col in row.primary_key]

                model = cls()
                for attr, value in result.items():
                    setattr(model, attr, value)
                results.append(model)

        return results
