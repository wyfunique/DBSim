from __future__ import absolute_import
import math
import inspect
from .function_wapper import FunctionWapper

def register_on(dataset):
  for name,func in inspect.getmembers(math, inspect.isbuiltin):
    dataset.add_function(
      name,
      FunctionWapper(name=name, func_body=func, returns=None), 
      returns=dict(name=name, type='NUMBER')
    )
