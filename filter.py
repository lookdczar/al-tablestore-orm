
from typing import List, Dict, Union
from .column import Column
from .primary_key import PrimaryKey
import tablestore

class Filter():
    def __init__(self, column: Union[Column, PrimaryKey], operators: List) -> None:
        self.column = column
        self.operator = operators

    def is_pk(self) -> bool:
        if isinstance(self.column, PrimaryKey):
            return True
        else:
            return False

    @classmethod
    def is_(cls, column: Union[Column, PrimaryKey], value):
        if isinstance(column, PrimaryKey):
            return Filter(column=column, operators=[(column.name, value), (column.name, value)]) 
        elif isinstance(column, Column):
            return Filter(column=column, operators=[tablestore.SingleColumnCondition(column.name, value, tablestore.ComparatorType.EQUAL)])
        
    @classmethod
    def not_(cls, column: Column, value):
            return Filter(column=column, operators=[tablestore.SingleColumnCondition(column.name, value, tablestore.ComparatorType.NOT_EQUAL)])

    @classmethod
    def in_(cls, column: Union[Column, PrimaryKey], min, max):
        if isinstance(column, PrimaryKey):
            return Filter(column=column, operators=[(column.name, min), (column.name, max)])
        elif isinstance(column, Column):
            return Filter(column=column, operators=[
                tablestore.SingleColumnCondition(column.name, min, tablestore.ComparatorType.GREATER_THAN),
                tablestore.SingleColumnCondition(column.name, max, tablestore.ComparatorType.LESS_THAN)
                ])
        
    @classmethod
    def larger_(cls, column: Union[Column, PrimaryKey], value):
        if isinstance(column, PrimaryKey):
            return Filter(column=column, operators=[(column.name, value), (column.name, tablestore.INF_MAX)])
        elif isinstance(column, Column):
            return Filter(column=column, operators=[tablestore.SingleColumnCondition(column.name, value, tablestore.ComparatorType.GREATER_THAN) ])
        
    @classmethod
    def less_(cls, column: Union[Column, PrimaryKey], value):
        if isinstance(column, PrimaryKey):
            return Filter(column=column, operators=[(column.name, value), (tablestore.INF_MIN, column.name)])
        elif isinstance(column, Column):
            return Filter(column=column, operators=[tablestore.SingleColumnCondition(column.name, value, tablestore.ComparatorType.LESS_THAN) ])
    
is_ = Filter.is_
not_ = Filter.not_
in_ = Filter.in_
larger_ = Filter.larger_
less_ = Filter.less_