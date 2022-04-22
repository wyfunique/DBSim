from typing import Callable, Union, Type
from enum import Enum

from .exceptions import *

class Comparison(Enum):
    LE = 1
    LEQ = 2
    GE = 3
    GEQ = 4
    EQ = 5
    NEQ = 6

def getClassNameOfInstance(obj):
    return type(obj).__name__

def getClassNameOfClass(cls):
    return cls.__name__

def getClass(obj):
    return obj.__class__

def isSubclassByClassName(cls, class_name: str):
    # all the ancestor classes of cls
    ancestor_classes = cls.mro()
    ancestor_class_names = set(list(map(getClassNameOfClass, ancestor_classes)))
    return class_name in ancestor_class_names

def isInstanceByClassName(obj, class_name: str):
    return isSubclassByClassName(getClass(obj), class_name)

def getFuncByName(instance_or_class: Union[object, Type], func_name: str) -> Callable:
  return getattr(instance_or_class, func_name)

def ERROR_IF_DIFF_TYPES(arg1, arg2, msg = None, exception_type = TypeError):
    if type(arg1) != type(arg2):
        if msg is None:
            msg = "arg1 and arg2 have different types."
        raise exception_type(msg)

def ERROR_IF_NOT_EQ(arg1, arg2, msg = None, exception_type = ValueError):
    ERROR_IF_DIFF_TYPES(arg1, arg2)
    if arg1 != arg2:
        if msg is None:
            msg = "arg1 and arg2 are not equal."
        raise exception_type(msg)

def ERROR_IF_COMPARISON_FALSE(arg1, arg2, comp: Comparison, msg = None, exception_type = ValueError):
    ERROR_IF_DIFF_TYPES(arg1, arg2)
    if comp == Comparison.LE and not (arg1 < arg2)\
        or comp == Comparison.LEQ and not (arg1 <= arg2)\
        or comp == Comparison.GE and not (arg1 > arg2)\
        or comp == Comparison.GEQ and not (arg1 >= arg2)\
        or comp == Comparison.EQ and not (arg1 == arg2)\
        or comp == Comparison.NEQ and not (arg1 != arg2):
        if msg is None:
            msg = "the comparison between arg1 and arg2 returns false."
        raise exception_type(msg)

def ERROR_IF_EXISTS_IN(arg1, arg2, msg = None, exception_type = RuntimeError):
    if arg1 in arg2:
        if msg is None:
            msg = "arg1 exists in arg2."
        raise exception_type(msg)

def ERROR_IF_NOT_INSTANCE_OF(obj, typename, msg = None, exception_type = TypeError):
    if not isinstance(obj, typename):
        if msg is None:
            msg = "obj is not an instance of the given type."
        raise exception_type(msg)

def ERROR_IF_NONE(arg, msg = None, exception_type = ValueError):
    if arg is None:
        if msg is None:
            msg = "arg is None."
        raise exception_type(msg)

def ERROR_IF_FALSE(statement, msg = None, exception_type = ValueError):
    if statement == False:
        if msg is None:
            msg = "statement is False."
        raise exception_type(msg)

def minus(set1: set, set2: set) -> set:
    ERROR_IF_NOT_INSTANCE_OF(set1, set, "'set1' is required to be a set ({} received)".format(type(set1)))
    ERROR_IF_NOT_INSTANCE_OF(set2, set, "'set2' is required to be a set ({} received)".format(type(set2)))
    return set1 - set2