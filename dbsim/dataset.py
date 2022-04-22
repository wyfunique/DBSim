from .query import Query, view_replacer
from .query_builder import QueryBuilder
from .query_parser import parse_statement
from .adapters.null_adapter import  NullAdapter
from .ast import LoadOp, Expr, AliasOp
from .aggregates import Aggregate
from .compilers import local

from .compilers.local import relational_function

from .field import Field

from .operations import walk

from . import functions
from . import aggregates

from . import compat

from . import config

class DataSet(object):

  def __init__(self, name = config.default_ds_name):
    self.name = name

    self.adapters = [NullAdapter()]

    self.relation_cache = {}

    self.views = {}
    self.schema_cache = {}
    self.executor = None
    self.compile = local.compile
    self.dump_func = None
    self.udfs = {}
    self.aggregates = {}
    
    functions.register_on(self)
    aggregates.register_on(self)

  def reset(self):
    self.adapters = [NullAdapter()]
    self.relation_cache = {}
    self.views = {}
    self.schema_cache = {}
    self.executor = None
    self.compile = local.compile
    self.dump_func = None
    self.udfs = {}
    self.aggregates = {}
    functions.register_on(self)
    aggregates.register_on(self)

  def add_adapter(self, adapter):
    """
    Add the given adapter to the dataset.

    Multiple adds for the instance of the same
    adapter are ignored.

    """

    if adapter not in self.adapters:
      self.adapters.append(adapter)
    return adapter

  def remove_adapter(self, adapter):
    """
    Remove the specified adapter from the dataset.
    """
    if adapter in self.adapters:
      self.adapters.remove(adapter)
    return adapter

  def create_view(self, name, query_or_operations):
    if isinstance(query_or_operations, compat.string_types):
      operations = self.query(query_or_operations).operations
    else:
      operations = query_or_operations

    self.views[name] = AliasOp(name,operations, operations.schema)
    
  def aggregate(self, returns=None, initial=None, name=None, finalize=None):
    def _(func, name):
      if name is None:
        name = func.__name__
      self.add_aggregate(name, func, returns, initial, finalize)
      return func
    return _

  def function(self, returns=None, name=None):
    """Decorator for registering functions

    @dataset.function
    def somefunction():
      pass


    """
    def _(func, name=name):
      if name is None:
        name = func.__name__
      self.add_function(name, func, returns)
      return func
    return _ 

  def add_aggregate(self, name, func, returns, initial, finalize=None):
    if name in self.udfs:
      # avoid registering an aggregate with the same name as any existing function
      raise ValueError("'{}' is already registered as a user-defined function.".format(name))

    self.aggregates[name] = Aggregate(
      name=name, 
      func_body=func, 
      returns=returns, 
      initial=initial,
      finalize=finalize
    )


  def add_function(self, name, function, returns=None):
    if name in self.aggregates:
      # avoid registering an user-defined function with the same name as any existing aggregate 
      raise ValueError("'{}' is already registered as an aggregate function.".format(name))

    if returns:
      if callable(returns):
        function.returns = returns
      else:
        function.returns = Field(**returns)
    else:
      function.returns = None

    self.udfs[name] = function

  # A uniform interface for getting both of aggregates and functions by name
  def get_function(self, name):
    function = self.udfs.get(name) or self.aggregates.get(name)
    if function:
      return function
    else:
      raise NameError("No function named {}".format(name))

  def get_view(self, name):
    return self.views.get(name)

    view = self.views.get(name)
    if view:
      return replace_views(view,self)
    else:
      return None

  @property
  def relations(self):
    """
    Returns a list of all relations from all adapters
    note this could be a slow operation as remote
    calls must be made to each adapter. 

    As a side effect the relation_cache will be updated.
    """

    for adapter in self.adapters:
      for name, schema in adapter.relations:
        self.relation_cache[name] = schema
    return [item for item in self.relation_cache.items() if item[0] != '']



  def adapter_for(self, relation_name: str):
    for adapter in self.adapters:
      if adapter.has(relation_name):
        return adapter
    raise RuntimeError("Relation '{}' not found in Dataset '{}'".format(relation_name, self.name))

  def get_relation(self, name) -> Expr:
    """Returns the relation for the given name.

    The dataset will search all adapters in the order the
    adapters were added to the dataset returning the first 
    relation with the given name.
    """
    relation = self.relation_cache.get(name)
    if relation is None:
      for adapter in self.adapters:
        relation = adapter.get_relation(name)
        if relation:
          self.relation_cache[name] = relation
          break

    return relation


  def get_schema(self, name):
    """
    Returns the schema for relation found with the given name.

    The search order is the same as dataset.get_relation()
    """
    return Query(self, LoadOp(name)).schema

  def set_compiler(self, compile_fun):
    self.compile = compile_fun

  def set_dump_func(self, dump_func):
    self.dump_func = dump_func

  def dump(self, schema, relation):
    self.dump_func(schema, relation)

  def execute(self, query, *params, **kw):

    callable = self.compile(query)
    default_ctx = {
      'dataset': self,
      'params': params
    }
    ctx = kw.get('ctx', default_ctx)

    return callable(ctx)


  def query(self, statement):
    """Parses the statement and returns a Query"""
    return Query(self, parse_statement(statement))

  def frm(self, relation_or_stmt):
    return QueryBuilder(self).frm(relation_or_stmt)

  def select(self, *cols):
    return QueryBuilder(self).select(*cols)


def replace_views(operation, dataset):
  def adapt(loc):
    node = loc.node()
    if isinstance(node, LoadOp):
      return view_replacer(dataset, loc, node)
    else:
      return loc
  return walk(operation, adapt)

