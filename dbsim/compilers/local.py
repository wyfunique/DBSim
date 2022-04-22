import operator
from functools import partial
from itertools import islice
import numbers

from ..ast import *
from .. import compat
from ..field import FieldType

from ..operations import  walk, visit_with, isa
from ..schema_interpreter import (
  field_from_expr,  JoinSchema, relational_function
)

from ..utils.logger import Logger

import inspect 
import typing

logger = Logger.general_logger
stat_field_in_ctx = 'stat'
num_input_rows_and_cost_factor_field = 'num_input_rows_and_cost_factor'

def addExecutor(
    executor: typing.Tuple[typing.Type[Expr], Callable], 
    dst_executor_table: typing.Dict[typing.Type[Expr], Callable]
):
  op_class, op_exe_func = executor
  if op_class in dst_executor_table:
    logger.warn("You are overwriting an existing executor: {} - {}"\
      .format(getClassNameOfClass(op_class), op_exe_func))
  dst_executor_table[op_class] = op_exe_func

def addExecutors(
  executors: typing.Dict[typing.Type[Expr], Callable], 
  dst_executor_table: typing.Dict[typing.Type[Expr], Callable]
):
  for op_class, op_exe_func in executors.items():
    addExecutor((op_class, op_exe_func), dst_executor_table)

def removeExecutor(
    executor: typing.Tuple[typing.Type[Expr], Callable], 
    dst_executor_table: typing.Dict[typing.Type[Expr], Callable]
  ):
  op_class, op_exe_func = executor
  if op_class in dst_executor_table and op_exe_func is dst_executor_table[op_class]:
    del dst_executor_table[op_class]
  else:
    logger.warn("You are removing a non-existing executor: {} - {}"\
      .format(getClassNameOfClass(op_class), op_exe_func))

def removeExecutors(
  executors: typing.Dict[typing.Type[Expr], Callable], 
  dst_executor_table: typing.Dict[typing.Type[Expr], Callable]
):
  for op_class, op_exe_func in executors.items():
    removeExecutor((op_class, op_exe_func), dst_executor_table)

def addDataTypes(extended_data_types: typing.Dict[typing.Type[Const], str]):
  """
  Adds execution functions for extended data types from the external
  """
  global VALUE_EXPR
  for nodetype in extended_data_types:
    if not issubclass(nodetype, Const):
      raise ExtensionInternalError(
        "compilers.local.addDataTypes only accepts subclasses of 'Const' ({} received)"\
          .format(type(nodetype))
      )
  executors = {nodetype: const_expr for nodetype in extended_data_types}
  addExecutors(executors, VALUE_EXPR)

def removeDataTypes(removed_data_types: typing.Dict[typing.Type[Const], str]):
  global VALUE_EXPR
  for nodetype in removed_data_types:
    if not issubclass(nodetype, Const):
      raise ExtensionInternalError(
        "compilers.local.removeDataTypes only accepts subclasses of 'Const' ({} received)"\
          .format(type(nodetype))
      )
  executors = {nodetype: const_expr for nodetype in removed_data_types}
  removeExecutors(executors, VALUE_EXPR)

def addPredicateOps(executors: typing.Dict[typing.Type[Expr], Callable]):
  """
  Adds execution functions for extended predicate operators 
    (i.e., the operators used in predicate expressions) from the external
  """
  global VALUE_EXPR
  addExecutors(executors, VALUE_EXPR)

def removePredicateOps(executors: typing.Dict[typing.Type[Expr], Callable]):
  global VALUE_EXPR
  removeExecutors(executors, VALUE_EXPR)

def addRelationOps(executors: typing.Dict[typing.Type[Expr], Callable]):
  """
  Adds execution functions for extended relational operators from the external
  """
  global RELATION_OPS
  addExecutors(executors, RELATION_OPS)
  
def removeRelationOps(executors: typing.Dict[typing.Type[Expr], Callable]):
  global RELATION_OPS
  removeExecutors(executors, RELATION_OPS)

def computeCost(ctx, op_node: Expr, input1: typing.Tuple, input2: typing.Tuple = None, aggregate_two_inputs = None) -> None:
  """
  Computes the number of processed rows for the current plan node 'op_node'
  and stores the results in context 'ctx' which can be accessed from the external by query optimizer.  

  Parameters
  ------------
  ctx: the context input from outside
  op_node: the current plan node
  input1 & input2: rows from the generators returned by the children executable, 
                    when op_node only has one child, input2 is None.
  aggregate_two_inputs: a callable function that aggregates the two inputs into one number of rows.
                    This allows the number of processed rows to be computed more flexibly. 
  """
  if stat_field_in_ctx not in ctx:
    ctx[stat_field_in_ctx] = dict()
  if num_input_rows_and_cost_factor_field not in ctx[stat_field_in_ctx]:
    ctx[stat_field_in_ctx][num_input_rows_and_cost_factor_field] = list()
  if aggregate_two_inputs is not None and input2 is not None:
    op_node.num_input_rows = aggregate_two_inputs(input1, input2)
    ctx[stat_field_in_ctx][num_input_rows_and_cost_factor_field].append((aggregate_two_inputs(input1, input2), op_node.cost_factor))
  else:
    ERROR_IF_NOT_INSTANCE_OF(input1, tuple)
    num_rows_input1 = len(input1) 
    num_rows_input2 = len(input2) if input2 is not None else 0
    op_node.num_input_rows = num_rows_input1 + num_rows_input2
    ctx[stat_field_in_ctx][num_input_rows_and_cost_factor_field].append((num_rows_input1 + num_rows_input2, op_node.cost_factor))

def old_div(a, b):
    """
    Equivalent to ``a / b`` on Python 2 without ``from __future__ import
    division``.
    """
    if isinstance(a, numbers.Integral) and isinstance(b, numbers.Integral):
        return a // b
    else:
        return a / b


def compile(query):
  # resolve views and schemas

  return walk(
    query.operations, 
    visit_with(
      query.dataset,      
      (isa(LoadOp), load_relation),
      (isa(ProjectionOp), ensure_group_op_when_ags),
      (isa_op, relational_op), # here the logical plan node is transformed to its physical executable
      (is_callable, validate_function)
    )
  )


def isa_op(loc):
  # check whether the current node is one of the RELATION_OPS
  return type(loc.node()) in RELATION_OPS

def relational_op(dataset, loc, operation):
  func = RELATION_OPS[type(operation)](dataset,  operation)
  func.schema = operation.schema
  # (1) The "replace" method of Loc is to replace the "current" attribute of Loc with the input parameter, 
  #     e.g., here the "current" attribute of the returned Loc instance is the "func".
  #     Since the "func" is the physical plan operator corresponding to the logical plan node, 
  #     so this method "relational_op" is actually translating the input logical plan to physical plan. 
  # (2) The "replace" method of Loc is not inplace, 
  #     i.e., here it does NOT change the Loc instance "loc", 
  #     instead, it returns an updated copy of "loc" with the replacement in effect, 
  #     while the original "loc" is unchanged.
  #     So the returned Loc instance from "relational_op" must be assigned to the "loc" variable in the main program to update it.
  return loc.replace(func)

def load_relation(dataset, loc, operation):
  adapter = dataset.adapter_for(operation.name)
  return adapter.evaluate(loc)

def is_callable(loc):
  return callable(loc.node())

def validate_function(dataset, loc, function):
  """
  Simple validation which ensure that nodes have been
  compiled to functions have a schema attribute set.
  """
  assert hasattr(function, 'schema'), (
    "{} must have a schema attribute".format(function)
  )
  return loc

def alias_op(dataset, operation):
  def alias(ctx):
    # relation is a generator over the output rows of the children nodes
    relation = operation.relation(ctx)

    # list(generator) will push the generator to the end, 
    #   so after calling 'list(relation)', 
    #   generator 'relation' has been on the end and has no next element.
    # Therefore we need to re-build the generator from the list before returning it.
    input_rows = list(relation)
    computeCost(ctx, operation, tuple(input_rows))
    # re-build the generator 'relation'
    relation = (row for row in input_rows)

    return relation

  return alias


def ensure_group_op_when_ags(dataset, loc, operation):
  aggs = aggregates(operation.exprs, dataset)
  if aggs:
    # we have an aggregate operations, push them up to the group by
    # operation or add a group by operation if all projection expresions
    # are aggregates
    u = loc.up()
    parent_op = u and u.node()
    if not isinstance(parent_op, GroupByOp):
      if len(aggs) != len(operation.exprs):
        raise group_by_error(operation.expresions, aggs)
      loc = loc.replace(GroupByOp(operation, aggregates=aggs)).down()
    else:
      loc = u.replace(parent_op.new(aggregates=aggs)).down()
  return loc


def projection_op(dataset,  operation):
  schema = operation.relation.schema
  columns = tuple([
    column
    for group in [
      column_expr(expr, schema, dataset)
      for expr in operation.exprs
    ]
    for column in group
  ])


  def projection(ctx):
    relation = operation.relation(ctx)

    input_rows = list(relation)
    computeCost(ctx, operation, tuple(input_rows))
    # re-build the generator 'relation'
    relation = (row for row in input_rows)

    return (
      tuple( col(row, ctx) for col in columns )
      for row in relation
    )
    
  return projection



def selection_op(dataset, operation):

  if operation.bool_op is None:
    return lambda relation, ctx: relation

  predicate  = value_expr(operation.bool_op, operation.schema, dataset)

  def selection(ctx):
    relation = operation.relation(ctx)

    input_rows = list(relation)
    computeCost(ctx, operation, tuple(input_rows))
    # re-build the generator 'relation'
    relation = (row for row in input_rows)

    return (
      row
      for row in relation
      if predicate(row, ctx)
    )
    
  return selection

def union_all_op(dataset, operation):

  def union_all(ctx):

    input_rows_left = list(operation.left(ctx))
    input_rows_right = list(operation.right(ctx))
    computeCost(ctx, operation, tuple(input_rows_left), tuple(input_rows_right))
    # re-build the two generators
    relation_left = (row for row in input_rows_left)
    relation_right = (row for row in input_rows_right)
    
    for row in relation_left:
      yield row

    for row in relation_right:
      yield row

  return union_all
  

def join_op(left_join, dataset, operation):
  left  = operation.left
  right = operation.right

  try:
    comparison = join_keys(left.schema, right.schema, operation.bool_op)
    # left inner join
    method = partial(hash_join, left_join)
  except ValueError:
    # icky cross product
    comparison = value_expr(operation.bool_op, operation.schema, dataset)
    method = nested_block_join

  def join(ctx):
    left = operation.left(ctx)
    right = operation.right(ctx)

    input_rows_left = list(left)
    input_rows_right = list(right)
    computeCost(
      ctx, operation, 
      tuple(input_rows_left), tuple(input_rows_right), 
      # The number of processed rows for JoinOp 
      # is not summation of the rows from two inputs, 
      # but multiplication of them.
      lambda l, r: len(l) * len(r)
    )
    # re-build the two generators
    left = (row for row in input_rows_left)
    right = (row for row in input_rows_right)

    return method(operation.left, operation.right, comparison, ctx)
    
  return join



def order_by_op(dataset, operation):
  columns = tuple(
    value_expr(expr, operation.relation.schema, dataset)
    for expr in operation.exprs
  )  
  schema = operation.schema

  def order_by(ctx):
    relation = operation.relation(ctx)

    input_rows = list(relation)
    computeCost(ctx, operation, tuple(input_rows))
    # re-build the generator 'relation'
    relation = (row for row in input_rows)

    def key(row):
      return tuple(
        compat.python2_sort_key(c(row, ctx)) for c in columns
      )

    return sorted(relation, key=key)
    
  return order_by

def group_by_op(dataset, group_op):

  if group_op.exprs:
    load   = order_by_op(dataset, group_op)
    load.schema = group_op.schema
  else:
    # we're aggregating the whole table
    load = group_op.relation 


  exprs      = group_op.exprs
  aggs       = group_op.aggregates

  initialize = initialize_op(aggs)
  accumalate = accumulate_op(aggs)
  finalize   = finalize_op(aggs)

  if group_op.exprs:
    key = key_op(group_op.exprs, load.schema)
  else:
    # it's all aggregates with no group by elements
    # so no need to order the table
    key = lambda row,ctx: None


  def group_by(ctx):
    ordered_relation = load(ctx)

    def group():
      records = iter(ordered_relation)

      row = next(records)
      group = key(row, ctx)
      
      tmp = initialize(row)

      record = accumalate(initialize(row), row)

      for row in records:
        next_ = key(row, ctx)
        if next_ != group:
          yield finalize(record)
          group = next_
          record = initialize(row)
        previous_row = accumalate(record, row)

      yield finalize(record)

    return group()
    
  return group_by


def slice_op(dataset, expr):
  def limit(ctx):
    relation = expr.relation(ctx)
    return islice(relation, expr.start, expr.stop)
    
  return limit



def is_aggregate(expr, dataset):
  """Returns true if the expr is an aggregate function."""
  if isinstance(expr, RenameOp):
    expr = expr.expr

  return isinstance(expr, Function) and expr.name in dataset.aggregates

def aggregate_expr(expr, dataset):
  if isinstance(expr, RenameOp):
    expr = expr.expr

  return dataset.aggregates[expr.name]

def group_by_error(exprs, aggs):
  """Used to raise an error highlighting the first
  offending column when a projection operation has a mix of
  aggregate expresions and non-aggregate expresions but no 
  group by.

  """

  agg_expr = dict(aggs)
  for col, expr in enumerate(exprs):
    if col not in aggs:
      return SyntaxError(
        (
          '"{}" must appear in the GROUP BY clause '
          'or be used in an aggregate function'
        ).format(expr)
      )


def key_op(exprs, schema):
  positions = tuple(
    schema.field_position(expr.path)
    for expr in exprs
  )
      
  def key(row, ctx):
    return tuple(row[pos] for pos in positions)
  return key

def initialize_op(pos_and_aggs):
  def initialize(row):
    # convert the tuple to a list so we can modify it
    record = list(row)
    for pos, agg in pos_and_aggs:
      record[pos] = agg.initial
    return record
  return initialize

def func_signature(func):
    """Helper function to get signature for
    user-defined or Python builtin callables.
    """
    # check if the input function object is a Python builtin method, using some internal method of 'inspect' package
    if inspect._signature_is_builtin(func):
      # check if the signature of this builtin method can be accessed by 'inspect'
      s = getattr(func, "__text_signature__", None)
      if not s: # signature of func is not accessible
        return None
      else: # use the API to get the signature, i.e., 'inspect.signature'
        return inspect.signature(func)
    else: # func is not a builtin but user defined method
      return inspect.signature(func)

def accumulate_op(pos_and_aggs):
  def accumulate(record, row):
    for pos, agg in pos_and_aggs:
      args = row[pos]
      state = record[pos]
      
      # Some aggregate functions only accept one input parameter for recording the accumulation state, 
      # like 'count' (whose body is 'lambda state: state + 1').
      # For such aggregates, an error will be thrown here as we pass more than one parameter, i.e., an arg 'state' and varargs 'args'.
      # So we have to check the number of accepted input parameters of the current aggregate, which can be done using 'inspect.signature'.
      
      # But note that 'inspect' package does NOT work on some built-in methods implemented by C, e.g., 'max' and 'min'.
      # Specifically, when initializing an 'inspect.signature' instance for built-in methods, 
      # the '__text_signature__' attr of the target built-in object will be obtained, 
      # if it is None or no such an attr existing, an exception will be thrown to say "no signature found for builtin".
      # So here we do the same thing to check if we can get the signature of the current aggregate function, 
      # if impossible, then we treat the current aggregate as accepting more than 1 parameter by default.
      # But always remember that unexpected cases could exist here. Now we just leave this risk here for later fix if we find a solution.     
      sig = func_signature(agg.func_body)
      if sig is not None:
        num_input_params = len(sig.parameters)
        if num_input_params == 0:
          raise Exception("Invalid aggregate definition: no input parameters accepted by the aggregate. \
            Aggregate function must have at least 1 input parameter for recording the accumulation state.")
        elif num_input_params == 1:
          record[pos] = agg.func_body(state)
        else:
          record[pos] = agg.func_body(state, *args)
      else:
        record[pos] = agg.func_body(state, *args)
    return record
  return accumulate

def finalize_op(pos_and_aggs):
  def finalize(record):
    # convert the tuple to a list so we can modify it
    for pos, agg in pos_and_aggs:
      if agg.finalize:
        state = record[pos]
        record[pos] = agg.finalize(state)
    return tuple(record)
  return finalize



def aggregates(exprs, dataset):
  """Returns a list of list or the aggregates in the exprs.

  The first item is the column index of the aggregate the
  second is the aggregate itself.
  """
  return tuple(
    (pos, aggregate_expr(aggr, dataset))
    for pos, aggr in enumerate(exprs)
    if is_aggregate(aggr, dataset)
  )


def column_expr(expr, schema, dataset):
  # selectall returns a group of expresions, group solo to be flattened
  # by the outer loop

  if isinstance(expr, SelectAllExpr):
    return select_all_expr(expr, schema, dataset)
  else: 
    return (value_expr(expr,schema,dataset),)

def select_all_expr(expr, schema, dataset):
  if expr.table is None:
    fields = schema.fields
  else:
    fields = [
      f
      for f in schema.fields
      if f.schema_name == expr.table
    ]

  return [
    var_expr(Var(f.path), schema, dataset) for f in fields
  ]

def value_expr(expr, schema, dataset):
  return VALUE_EXPR[type(expr)](expr, schema, dataset)

def itemgetter_expr(expr, schema, dataset):
  key = expr.key
  def itemgetter(row, ctx):
    return row[key]
  return itemgetter

def sub_expr(expr, schema, dataset):
  return value_expr(expr.expr, schema, dataset)

def var_expr(expr, schema, dataset):
  pos = schema.field_position(expr.path)
  def var(row, ctx):
    return row[pos]
  return var

def const_expr(expr, schema, dataset):
  def const(row, ctx):
    return expr.const
  return const

def null_expr(expr, schema, dataset):
  return lambda row, ctx: None

def function_expr(expr, schema, dataset):
  function = dataset.get_function(expr.name)
  arg_exprs = tuple(
    value_expr(arg_expr, schema, dataset)
    for arg_expr in expr.args
  )

  def _(row, ctx):
    args = tuple(
      arg_expr(row, ctx)
      for arg_expr in arg_exprs
    )
    return function(*args)

  _.__name__ = str(expr.name)
  return _

def param_getter_expr(expr, schema, dataset):
  # todo: params binding are delayed... i.e.
  # we don't know what they are until after the
  # query has been parsed. so error checking here
  # is horrible. Maybe we could introduce a generic type
  # that is substitude out at the start of query.evaluate
  pos = expr.expr
  def get_param(record, ctx):
    return ctx.get('params', [])[pos]
  return get_param


def desc_expr(expr, schema, dataset):
  field = field_from_expr(expr.expr, dataset, schema)
  value = value_expr(expr.expr, schema, dataset)

  if field.type in (FieldType.INTEGER, FieldType.FLOAT, ):

    def invert_number(record, ctx):
      return -value(record,ctx)

    return invert_number

  elif field.type == FieldType.STRING:
    def invert_string(record, ctx):
      return [-b for b in bytearray(value(record,ctx))]
    return invert_string
  else:
    return lambda r,c: None
 
def unary_op(operator, expr, schema, dataset):
  val = value_expr(expr.expr, schema, dataset)
  def _(row,ctx):
    return operator(val(row, ctx))
  _.__name__ = operator.__name__
  return _

def binary_op(operator, expr, schema, dataset):
  lhs = value_expr(expr.lhs, schema, dataset)
  rhs = value_expr(expr.rhs, schema, dataset)

  def _(row, ctx):
    return operator(lhs(row, ctx), rhs(row, ctx))
  _.__name__ = operator.__name__
  return _



VALUE_EXPR = {
  Var: var_expr,
  StringConst: const_expr,
  NumberConst: const_expr,

  NullConst: const_expr,
  TrueConst: const_expr,
  FalseConst: const_expr,

  ParamGetterOp: param_getter_expr,

  Function: function_expr,

  NegOp: partial(unary_op, operator.neg),
  NotOp: partial(unary_op, operator.not_),

  And: partial(binary_op, operator.and_),
  Or: partial(binary_op, operator.or_),

  LtOp: partial(binary_op, operator.lt),
  LeOp: partial(binary_op, operator.le),
  EqOp: partial(binary_op, operator.eq),
  NeOp: partial(binary_op, operator.ne),
  GeOp: partial(binary_op, operator.ge),
  GtOp: partial(binary_op, operator.gt),
  IsOp: partial(binary_op, operator.is_),
  IsNotOp: partial(binary_op, operator.is_not),

  AddOp: partial(binary_op, operator.add),
  SubOp: partial(binary_op, operator.sub),

  MulOp: partial(binary_op, operator.mul),
  DivOp: partial(binary_op, old_div),

  ItemGetterOp: itemgetter_expr,

  RenameOp: sub_expr,
  NullConst: null_expr,

  Asc: sub_expr,
  Desc: desc_expr

}
"""
VALUE_EXPR stores the mapping: predicate operator or value node -> its compilation function.

The compilation function is such a function: (expr, schema, dataset) -> ((row, ctx) -> Any)
  i.e., inputs predicate expression tree, the schema of the tree nodes, the dataset being used, 
        outputs a function (acting as an executable) 
                that takes a table row/record and some context as inputs 
                and outputs the execution results over the row/record 
                (the result type may vary in different situations).

Those compilation functions will be called by the relational operator compilers present in RELATION_OPS.
"""

RELATION_OPS = {
  AliasOp: alias_op,
  ProjectionOp: projection_op,
  SelectionOp: selection_op,
  OrderByOp: order_by_op,
  GroupByOp: group_by_op,
  SliceOp: slice_op,
  JoinOp: partial(join_op, False),
  UnionAllOp: union_all_op,
  LeftJoinOp: partial(join_op, True),
  Function: relational_function,
  
}
"""
RELATION_OPS stores the mapping: relational operator -> its compilation function.

The compilation function is such a function: (dataset,  operation) -> ((ctx) -> Any)
  i.e., inputs the dataset being used, resolved logical plan,  
        outputs a function (acting as an executable) 
                that takes some context as input 
                and outputs the execution results over the dataset.
"""

# sigh, oh python and your circular import
from .join import nested_block_join, hash_join, join_keys
