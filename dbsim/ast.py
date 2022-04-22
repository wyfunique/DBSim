import typing
from typing import List, Set, Dict, Union
from copy import copy, deepcopy
from enum import Enum
from .immutable import ImmutableMixin
from collections import namedtuple

from .utils import *
from .utils.exceptions import *

import inspect
import queue

DEFAULT_COST_FACTOR: float = 1.0
TINY_COST_FACTOR: float = 0.1

class ResolveStatus(Enum):
  RESOLVED = 1
  UNRESOLVED = 2
  UNKNOWN = 3

class Expr(ImmutableMixin):
  """
    cost_factor
    --------------
    A factor for enlarging or shrinking the logical cost.
    The following formula is used to compute logical cost of a relational operator:

        logical_cost = number_of_processed_rows * cost_factor

    The initial/default cost_factor is 1.0; 
      for operators with heavier computation workload, the factor > 1.0;
      for those with lighter workload, factor < 1.0 .    
    After the schema is resolved, the cost_factor will be refined based on the initial value.
  """

  def __eq__(self, other):
    """Compare two Exprs by values of attributes, instead of by reference"""
    result = (
      isinstance(other, self.__class__)
      and all(
        getattr(self, attr) == getattr(other, attr)
        for attr in self.__slots__
        if attr != 'schema'
      )
    )

    return result

  def __ne__(self, other):
    return not self == other

  def equal(self, other, ignore_schema: bool = True, match_loadop_and_relation: bool = False) -> bool:
    # if ignore_schema is True, the 'schema' is excluded from the attr comparison;
    # if match_loadop_and_relation is True, we consider LoadOp and Relation as equal.
    if getClass(self) == Relation and getClass(other) == LoadOp\
     or getClass(self) == LoadOp and getClass(other) == Relation:
      return match_loadop_and_relation and self.name  == other.name
    if getClass(self) == Relation and getClass(other) == Relation:
      return self.adapter == other.adapter and self.name == other.name
    
    result = (
      getClass(other) == getClass(self)
      and all(
        (
          isinstance(getattr(self, attr), Expr) and getattr(self, attr).equal(getattr(other, attr), ignore_schema, match_loadop_and_relation)
          or 
          not isinstance(getattr(self, attr), Expr) and getattr(self, attr) == (getattr(other, attr))
        )
        for attr in self.__slots__
        if not ignore_schema or attr != 'schema'
      )
    )
    return result

  def notEqual(self, other, ignore_schema: bool = True, match_loadop_and_relation: bool = False) -> bool:
    return not self.equal(other, ignore_schema, match_loadop_and_relation) 
  
  def _getResolveStatus(self) -> ResolveStatus:
    if not hasattr(self, 'schema'):
      return ResolveStatus.UNKNOWN
    else:
      if self.schema is None:
        return ResolveStatus.UNRESOLVED
      else:
        return ResolveStatus.RESOLVED

  def isResolved(self) -> bool:
    """Returns True if this expression tree is resolved (i.e., with not-None schemas)"""
    resolve_status = self._getResolveStatus() 
    if resolve_status != ResolveStatus.UNKNOWN:
      return True if resolve_status == ResolveStatus.RESOLVED else False
    else:
      # continue to check the child nodes until finding the first whose resolve status is not unknown
      for child in getChildren(self):
        return child.isResolved()
      raise RuntimeError("{}.isResolved() should never reach here (if it did, that was probably caused by an invalid expression tree)"\
        .format(getClassNameOfInstance(self)))

  def getCostFactor(self) -> float:
    if hasattr(self, 'cost_factor'):
      return self.cost_factor
    else:
      return None

  def setCostFactor(self, new_cost_factor: float) -> bool:
    ERROR_IF_NOT_INSTANCE_OF(new_cost_factor, (int, float), 
      "requires an integer or float value for cost_factor ({} received)".format(type(new_cost_factor)))
    if hasattr(self, 'cost_factor'):
      self.cost_factor = new_cost_factor
      return True
    else:
      return False

  def getNumInputRows(self) -> float:
    if hasattr(self, 'num_input_rows'):
      return self.num_input_rows
    else:
      return None

  def __deepcopy__(self, memo):
    cls = self.__class__
    result = cls.__new__(cls)
    memo[id(self)] = result
    for attr, val in inspect.getmembers(self):
      if not attr.startswith("__") and not inspect.ismethod(val): 
        setattr(result, attr, deepcopy(val, memo))
    return result

  def __str__(self):
    raise NotImplementedError

class SimpleOp(Expr):
  __slots__ = ('cost_factor', )
  """
  The non-relational operators that work on simple calculation or logical expressions, 
  like '<', 'AND', '+', etc.
  """
  pass

class UnaryOp(SimpleOp):
  __slots__ = ('expr',)
  def __init__(self, expr, cost_factor: float = DEFAULT_COST_FACTOR):
    self.cost_factor = cost_factor
    self.expr = expr
  def __str__(self):
    raise NotImplementedError

class NegOp(UnaryOp):
  """ - (expr)"""
  __slots__ = ('expr',)
  def __str__(self):
    return "-{}".format(str(self.expr))

class NotOp(UnaryOp):
  """ not (expr)"""
  __slots__ = ('expr',)
  def __str__(self):
    return "NOT {}".format(str(self.expr))

class ItemGetterOp(Expr):
  """ (expr)[key] """
  __slots__ = ('key',)
  def __init__(self, key, cost_factor: float = DEFAULT_COST_FACTOR):
    self.cost_factor = cost_factor
    self.key = key
  

class BinaryOp(SimpleOp):
  __slots__ = ('lhs', 'rhs')

  def __init__(self, lhs, rhs, cost_factor: float = DEFAULT_COST_FACTOR):
    self.cost_factor = cost_factor
    self.lhs = lhs
    self.rhs = rhs
  
  def __str__(self):
    raise NotImplementedError

class And(BinaryOp):
  """lhs and rhs"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} AND {}".format(str(self.lhs), str(self.rhs))

class Or(BinaryOp):
  """lhs or rhs"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} OR {}".format(str(self.lhs), str(self.rhs))

class LtOp(BinaryOp):
  """Less than"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} < {}".format(str(self.lhs), str(self.rhs))

class LeOp(BinaryOp):
  """Less than or equal to"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} <= {}".format(str(self.lhs), str(self.rhs))

class EqOp(BinaryOp):
  """Equal to"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} = {}".format(str(self.lhs), str(self.rhs))

class NeOp(BinaryOp):
  """Not equal to"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} != {}".format(str(self.lhs), str(self.rhs))

class GeOp(BinaryOp):
  """Greater than or equal to"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} >= {}".format(str(self.lhs), str(self.rhs))

class GtOp(BinaryOp):
  """Greater than"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} > {}".format(str(self.lhs), str(self.rhs))

class IsOp(BinaryOp):
  """x is y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} IS {}".format(str(self.lhs), str(self.rhs))

class IsNotOp(BinaryOp):
  """x is not y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} IS NOT {}".format(str(self.lhs), str(self.rhs))

class LikeOp(BinaryOp):
  """x LIKE y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} LIKE {}".format(str(self.lhs), str(self.rhs))

class NotLikeOp(BinaryOp):
  """x NOT LIKE y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} NOT LIKE {}".format(str(self.lhs), str(self.rhs))

class NotRLikeOp(BinaryOp):
  """x Not RLIKE y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} NOT RLIKE {}".format(str(self.lhs), str(self.rhs))

class RLikeOp(BinaryOp):
  """x RLIKE y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} RLIKE {}".format(str(self.lhs), str(self.rhs))

class RegExpOp(BinaryOp):
  """x RLIKE y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} RegExp {}".format(str(self.lhs), str(self.rhs))

class InOp(BinaryOp):
  """x in y"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} IN {}".format(str(self.lhs), str(self.rhs))

class AddOp(BinaryOp):
  """lhs + rhs"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} + {}".format(str(self.lhs), str(self.rhs))

class SubOp(BinaryOp):
  """lhs - rhs"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} - {}".format(str(self.lhs), str(self.rhs))

class MulOp(BinaryOp):
  """lhs * rhs"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} * {}".format(str(self.lhs), str(self.rhs))

class DivOp(BinaryOp):
  """lhs / rhs"""
  __slots__ = ('lhs', 'rhs')
  def __str__(self):
    return "{} / {}".format(str(self.lhs), str(self.rhs))

class BetweenOp(BinaryOp):
  """ x between lhs and rhs"""
  __slots__ = ( 'expr', 'lhs', 'rhs')
  def __init__(self, expr, lhs, rhs, cost_factor: float = DEFAULT_COST_FACTOR):
    self.cost_factor = cost_factor
    self.expr = expr
    self.lhs  = lhs
    self.rhs  = rhs
  def __str__(self):
    return "{} BETWEEN {} AND {}".format(str(self.expr), str(self.lhs), str(self.rhs))

class Value(SimpleOp):
  __slots__ = ()
  """
  The expressions that can act as the values of SimpleOps, 
  like constants, variables, functions, etc.

  Value is a subclass of SimpleOp 
    because some values themsleves can act as a predicate, 
    like TrueConst and FalseConst. 
  """
  pass

class Const(Value):
  __slots__ = ('const',)

  def __init__(self, const):
    # constant node has very little logical cost 
    self.cost_factor = TINY_COST_FACTOR
    self.const = const
  def __str__(self):
    return str(self.const)

class NullConst(Const):
  """Null or None"""
  __slots__ = ()
  const = None
  def __init__(self):
    self.cost_factor = 0.0
    pass


class NumberConst(Const):
  """Integer or Float"""
  __slots__ = ('const',)

class StringConst(Const):
  """A string"""
  __slots__ = ('const',)
  def __str__(self):
    return "'{}'".format(str(self.const))

class BoolConst(Const):
  """A boolean const"""
  __slots__ = ()
  def __init__(self):
    self.cost_factor = TINY_COST_FACTOR
    pass

class TrueConst(BoolConst):
  """The constant True """
  __slots__ = ()
  const=True

class FalseConst(BoolConst):
  """The constant False """
  __slots__ = ()
  const=False


class Var(Value):
  __slots__ = ('path',)
  def __init__(self, path):
    self.cost_factor = TINY_COST_FACTOR
    self.path = path
  def __str__(self):
    return str(self.path)

class Tuple(Value):
  __slots__ = ('exprs',)
  def __init__(self, *exprs):
    self.cost_factor = TINY_COST_FACTOR
    self.exprs = exprs
  def __str__(self):
    # TODO: to complete this
    raise NotImplementedError

class Function(Value):
  __slots__ = ('name', 'args', 'schema', 'func')
  def __init__(self, name, *args, **kw):
    self.cost_factor=kw.get('cost_factor', DEFAULT_COST_FACTOR)
    self.name = name
    self.args = args
    self.schema = kw.get('schema')
    self.func = kw.get('func')

  def __call__(self,ctx):
    return self.func(ctx)

  def __str__(self):
    func_name = self.name
    func_arg_list = "(" + ', '.join(list(map(str, self.args))) + ")"
    return func_name + func_arg_list
  

COMPARISON_OPS = {
  '<'  : LtOp,
  '<=' : LeOp,
  '='  : EqOp,
  '!=' : NeOp,
  '>=' : GeOp,
  '>'  : GtOp,
  'is' : IsOp,
  'is not' : IsNotOp,
  'like' : LikeOp,
  'rlike': RLikeOp,
  'not like' : NotLikeOp,
  'not rlike' : NotRLikeOp,
  'regexp': RegExpOp

}


MULTIPLICATIVE_OPS ={
  '*'  : MulOp,
  '/'  : DivOp
}

ADDITIVE_OPS = {
  '+'  : AddOp,
  '-'  : SubOp
}

# sql specific expresions

class Asc(UnaryOp):
  """Sort from lowest to highest"""
  __slots__ = ('expr',)
  def __str__(self):
    return "ASC"

class Desc(UnaryOp):
  """Sort from highest to lowest """
  __slots__ = ('expr',)
  def __str__(self):
    return "DESC"

class ParamGetterOp(UnaryOp):
  """ ?<number> """
  __slots__ = ('expr',)
  

class SelectAllExpr(Expr):
  __slots__ = ('table', 'cost_factor')

  def __init__(self, table=None, cost_factor: float = DEFAULT_COST_FACTOR):
    self.cost_factor = cost_factor
    self.table = table

class RenameOp(Expr):
  __slots__ = ('name','expr', 'cost_factor')

  def __init__(self, name, expr, cost_factor: float = DEFAULT_COST_FACTOR):
    self.cost_factor = cost_factor
    self.name = name
    self.expr  = expr


class Predicate(SimpleOp):
  """
  Represents a predicate expression and provides related utilities.
  Note that this is not a regular operator and will never appear in an AST, 
    instead, it is only used for analytic and debugging purposes.
  """
  __slots__ = ("sources", "base_expr")

  def __init__(self, expression: SimpleOp):
    ERROR_IF_NOT_INSTANCE_OF(
      expression, SimpleOp, 
      "requires the base expression to be a SimpleOp ({} received)".format(type(expression))
    )
    self.base_expr = expression
    self.sources: Set[str] = set() # Set[source_column1, source_column2, ...]
    for op in traverse(self.base_expr):
      self._extractSources(op)
  
  @classmethod
  def fromRelationOp(cls, op: 'SuperRelationalOp') -> 'Predicate':
    ERROR_IF_FALSE(
      hasattr(op, 'bool_op'), 
      "predicate expression not found (operator type: {})".format(type(op)), 
      PlannerInternalError
    )
    return cls(op.bool_op)

  def __eq__(self, other) -> bool:
    if isinstance(other, Predicate):
      return self.base_expr == other.toExpr()
    if isinstance(other, Expr) and not isinstance(other, Predicate):
      return self.base_expr == other
    return False

  def __ne__(self, other) -> bool:
    return not self == other

  def __str__(self) -> str:
    return "<Predicate: {}>".format(str(self.base_expr))

  def __hash__(self):
    return hash(str(self))

  def _extractSources(self, op: Expr):
    if isinstance(op, Value):
      if isinstance(op, Var):
        self.sources.add(op.path)
      elif isinstance(op, Const):
        # do nothing
        pass 
      else:
        raise NotImplementedError(
          "Unsupported predicate elements. Please do not use tuple or function in the predicate currently."
        )
  
  def getSources(self):
    """
    Returns the source columns info from this predicate. 
    If no column is specified, 
      like 'select * from table where 1 = 1', where the SelectionOp.bool_op is a TrueConst, 
      this method simply returns an empty set. 
    """
    return self.sources 
  
  def toExpr(self) -> Expr:
    return self.base_expr

  def equalToExprByStr(self, expr_str: str) -> bool:
    """Checks if this predicate can be represented as the given expression string"""
    return equalPredicate(self.toExpr(), expr_str)

  def relatedTo(self, plan_node: Expr) -> bool:
    """
    Checks if this predicate involves columns in the schema of a given resolved plan node.
    This method is critical for rules like predicate-push-down 
      which needs to know whether to push down the predicate towards left or right child node.
    """
    ERROR_IF_NOT_INSTANCE_OF(
      plan_node, Expr, 
      "requires an expression tree (Expr) as input ({} received)".format(type(plan_node))
    )
    ERROR_IF_FALSE(
      plan_node.isResolved(), 
      "received unresolved plan rooted at a {}".format(type(plan_node))
    )
    for column_path in self.getSources():
      try:
        plan_node.schema.get_field(column_path)
        return True
      except Exception as e:
        if isinstance(e, FieldNotFoundError):
          continue
        else:
          raise e
    return False


"""
  'RelationalOp' is the base class for operators with only one input relation/child, 
  of which the child is stored by 'relation' attr, except LoadOp, CaseWhenOp and CastOp.

  'BinRelationalOp' is the base class for operators with two input relations/children, 
  of which the children are stored by 'left' and 'right' attrs.

  'SuperRelationalOp' is the super class for both of RelationalOp and BinRelationalOp. 
  We use it only to ease the type-checking of AST nodes.
"""

class SuperRelationalOp(Expr):
  def getStrFormat(self) -> str:
    return "{} : {}"
  def getOpName(self) -> str:
    return getClassNameOfInstance(self).replace('Op', '')
  def getPredicate(self) -> Predicate:
    ERROR_IF_FALSE(hasattr(self, 'bool_op'), 
      "predicate not found ({} has no attribute 'bool_op')".format(getClassNameOfInstance(self)), 
      PlannerInternalError
    )
    return Predicate(self.bool_op)

class RelationalOp(SuperRelationalOp):
  def __str__(self):  
    raise NotImplementedError

# TODO: add __str__ for each following Op

class LoadOp(RelationalOp):
  """Load a relation with the given name"""
  __slots__ = ('name','schema', 'cost_factor', 'num_input_rows')
  def __init__(self, name, schema=None, num_input_rows=-1):
    self.cost_factor = 0.0
    self.name = name
    self.schema = schema
    self.num_input_rows=-1
  def __str__(self):
    operand = self.name
    return self.getStrFormat().format(self.getOpName(), operand)

class DummyOp(RelationalOp):
  """
  Just a dummy operator. 
  It does nothing except being inherited by Relation, 
    such that Relation can be better mixed in a resolved expression tree
    as a special Op node.  
  """
  pass

class Relation(namedtuple('Relation', 'adapter, name, schema, records'), DummyOp):
  """
  Represents a list of tuples
  """
  __slots__ = ()
  
  def __deepcopy__(self, memo):
    cls = self.__class__
    args_list_for_new = [cls]
    i_param = 0
    for param_name in inspect.signature(cls.__new__).parameters:
      if i_param == 0:
        i_param += 1
        continue
      else:
        args_list_for_new.append(eval("self.{}".format(param_name)))
    result = cls.__new__(*args_list_for_new)
    return result

  def __call__(self, ctx):
    return self.records(ctx)

  def __str__(self):
    operand = self.name
    return self.getStrFormat().format(self.getOpName(), operand)

class AliasOp(RelationalOp):
  """Rename the relation to the given name"""
  __slots__ = ('relation', 'name', 'schema', 'cost_factor', 'num_input_rows')
  def __init__(self, name, relation, schema=None, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    self.cost_factor = cost_factor
    self.name = name
    self.relation = relation
    self.schema = schema
    self.num_input_rows = -1




class ProjectionOp(RelationalOp):
  __slots__ = ('relation', 'exprs', 'schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation, *exprs, **kw):
    self.relation = relation
    # 'exprs' is the returning list of 'select_core_exp' method in query_parser_toolbox.py
    self.exprs = exprs
    self.schema = kw.get('schema')
    self.cost_factor = kw.get('cost_factor', DEFAULT_COST_FACTOR)
    self.num_input_rows = -1
  def __str__(self):
    tmp = []
    for expr in self.exprs:
      tmp.append(str(expr))
    operand = ', '.join(tmp)
    return self.getStrFormat().format(self.getOpName(), operand)
    
class SelectionOp(RelationalOp):
  __slots__ = ('relation','bool_op','schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation, bool_op, schema=None, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    self.relation = relation
    self.bool_op = bool_op
    self.schema = schema
    self.cost_factor = cost_factor
    self.num_input_rows = -1
  def __str__(self):
    operand = str(self.bool_op)
    return self.getStrFormat().format(self.getOpName(), operand)
    
class CaseWhenOp(RelationalOp):
  __slots__ = ('conditions', 'default_value', 'cost_factor', 'num_input_rows')
  def __init__(self, conditions, default_value, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    self.conditions = conditions
    self.default_value = default_value
    self.cost_factor = cost_factor
    self.num_input_rows = -1

class CastOp(RelationalOp):
  __slots__ = ('expr', 'type', 'cost_factor', 'num_input_rows')
  def __init__(self, expr, type, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    self.expr = expr
    self.type = type
    self.cost_factor = cost_factor
    self.num_input_rows = -1

class BinRelationalOp(SuperRelationalOp):
  """
  RelationalOp that operates on two relations
  """
  __slots__ = ('left', 'right', 'schema', 'cost_factor')

class UnionAllOp(BinRelationalOp):
  """Combine the results of multiple operations with identical schemas
  into one.
 """
  __slots__ = ('left', 'right', 'schema', 'cost_factor', 'num_input_rows')
  def __init__(self, left, right, schema=None, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    self.left = left
    self.right = right
    self.schema = schema
    self.cost_factor = cost_factor
    self.num_input_rows = -1

class JoinOp(BinRelationalOp):
  __slots__ = ('left','right', 'bool_op', 'schema', 'cost_factor', 'num_input_rows')
  def __init__(self,  left, right, bool_op = TrueConst(), schema=None, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    self.left = left
    self.right = right
    self.bool_op = bool_op
    self.schema = schema
    self.cost_factor = cost_factor
    self.num_input_rows = -1
  def __str__(self):
    operand = str(self.bool_op)
    return self.getStrFormat().format(self.getOpName(), operand)
    
class LeftJoinOp(JoinOp):
  __slots__ = ('left','right', 'bool_op', 'schema', 'cost_factor', 'num_input_rows')

class OrderByOp(RelationalOp):
  __slots__ = ('relation', 'exprs', 'schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation, first, *exprs, **kw):
    self.relation = relation
    self.exprs = (first,) + exprs
    self.schema = kw.get('schema')
    self.cost_factor = kw.get('cost_factor', DEFAULT_COST_FACTOR)
    self.num_input_rows = -1

  def new(self, **parts):
    # OrderByOp's __init__ doesn't match what's defined in __slots__
    # so we have to help it make a copy of this object
    exprs = parts.pop('exprs', self.exprs)
    first = exprs[0]
    tail = exprs[1:]
    relation = parts.pop('relation', self.relation)

    return self.__class__(relation, first, *tail, **parts)


class GroupByOp(RelationalOp):
  __slots__ = ('relation','aggregates','exprs','schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation,  *exprs, **kw):
    self.relation   = relation
    self.exprs      = exprs

    # bit of a kludge, aggregates can't be resolved at
    # parse time, so they start as an empty list and
    # are set when the expression tree is evaluated.
    # See compilers.local.projection_op for details
    self.aggregates = kw.get('aggregates', ())

    self.schema = kw.get('schema')
    self.cost_factor = kw.get('cost_factor', DEFAULT_COST_FACTOR)
    self.num_input_rows = -1

class SliceOp(RelationalOp):
  __slots__ = ('relation','start','stop', 'schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation, *args, **kw):
    self.relation = relation
    if len(args) == 1:
      self.start = 0
      self.stop = args[0]
    else:
      self.start, self.stop = args

    self.schema = kw.get('schema')
    self.cost_factor = kw.get('cost_factor', DEFAULT_COST_FACTOR)
    self.num_input_rows = -1
  def new(self, **parts):
    # slice op's __init__ doesn't match what's defined in __slots__
    # so we have to help it make a copy of this object
    args = parts.pop('start', self.start), parts.pop('stop', self.stop)
    relation = parts.pop('relation', self.relation)
 
    return self.__class__(relation, *args, **parts)


def getChildren(node: Expr) -> List[Expr]:
  ERROR_IF_NONE(node, "getChildren(node) received NoneType")
  ERROR_IF_NOT_INSTANCE_OF(node, Expr, 
    "getChildren(node) only accepts an Expr as input ({} received)".format(type(node))
  )
  if isinstance(node, (LoadOp, Relation, Value)):
    # LoadOp, Relation and Value nodes all have no children
    return []
  if isinstance(node, UnaryOp) and hasattr(node, "expr"):
    return [node.expr]
  if isinstance(node, BinaryOp) and hasattr(node, "lhs"):
    return [node.lhs, node.rhs]
  if isinstance(node, RelationalOp) and hasattr(node, "relation"):
    return [node.relation]
  if isinstance(node, BinRelationalOp) and hasattr(node, "left"):
    return [node.left, node.right]
  raise NotImplementedError("has not yet supported this type of AST node")

def setChildren(node: Expr, children: List[Expr], child_indices: List[int]) -> None:
  ERROR_IF_NONE(node, "'setChildren' received 'node' of NoneType")
  ERROR_IF_NONE(children, "'setChildren' received 'children' of NoneType")
  ERROR_IF_NONE(child_indices, "'setChildren' received 'child_indices' of NoneType")
  ERROR_IF_NOT_INSTANCE_OF(node, Expr, 
    "setChildren(node) only accepts an Expr as input ({} received)".format(type(node))
  )
  ERROR_IF_NOT_EQ(len(children), len(child_indices), 
    "lists 'children' and 'child_indices' have different numbers of elements"
  )
  if isinstance(node, (LoadOp, Relation, Value)):
    # LoadOp, Relation and Value nodes all have no children
    return
  if isinstance(node, UnaryOp) and hasattr(node, "expr"):
    assert len(children) == 1
    node.expr = children[0]
    return 
  if isinstance(node, BinaryOp) and hasattr(node, "lhs"):
    assert len(children) == 1 and len(child_indices) == 1 or len(children) == 2 and len(child_indices) == 2
    if len(children) == 1:
      if child_indices[0] == 0:
        node.lhs = children[0]
      elif child_indices[0] == 1:
        node.rhs = children[1]
      else:
        raise ValueError("invalid values in 'child_indices'")
    else:
      node.lhs, node.rhs = children[0], children[1]
    return
  if isinstance(node, RelationalOp) and hasattr(node, "relation"):
    assert len(children) == 1
    node.relation = children[0]
    return
  if isinstance(node, BinRelationalOp) and hasattr(node, "left"):
    assert len(children) == 1 and len(child_indices) == 1 or len(children) == 2 and len(child_indices) == 2
    if len(children) == 1:
      if child_indices[0] == 0:
        node.left = children[0]
      elif child_indices[0] == 1:
        node.right = children[1]
      else:
        raise ValueError("invalid values in 'child_indices'")
    else:
      node.left, node.right = children[0], children[1]
    return
  raise NotImplementedError("has not yet supported this type of AST node")

def deepCopyAST(ast_root: Expr) -> Expr:
  """DFS to traverse and deep copy each AST node"""
  if ast_root is None:
    return None
  ERROR_IF_NOT_INSTANCE_OF(ast_root, Expr, 
    "'ast_root' is required to be an instance of ast.Expr ({} received).".format(type(ast_root))
  )
  # deep copy the current root node
  copy_ast_root = deepcopy(ast_root)
  if isinstance(ast_root, (LoadOp, Value)):
    # LoadOp and Value nodes both have no children
    return copy_ast_root
  ast_subtrees = getChildren(ast_root)
  # recursively deep copy the subtrees 
  copy_ast_subtrees = [deepCopyAST(subtree) for subtree in ast_subtrees]
  # connect copy of children to copy of root
  if isinstance(ast_root, SimpleOp):
    if isinstance(ast_root, UnaryOp) and hasattr(ast_root, "expr"):
      assert len(copy_ast_subtrees) == 1
      copy_ast_root.expr = copy_ast_subtrees[0]
    elif isinstance(ast_root, BinaryOp) and hasattr(ast_root, "lhs"):
      assert len(copy_ast_subtrees) == 2
      copy_ast_root.lhs = copy_ast_subtrees[0]
      copy_ast_root.rhs = copy_ast_subtrees[1]
  elif isinstance(ast_root, SuperRelationalOp):
    if isinstance(ast_root, RelationalOp) and hasattr(ast_root, "relation"):
      assert len(copy_ast_subtrees) == 1
      copy_ast_root.relation = copy_ast_subtrees[0]
    elif isinstance(ast_root, BinRelationalOp) and hasattr(ast_root, "left"):
      assert len(copy_ast_subtrees) == 2
      copy_ast_root.left = copy_ast_subtrees[0]
      copy_ast_root.right = copy_ast_subtrees[1]
  else:
    raise NotImplementedError("has not yet supported {}".format(getClassNameOfInstance(ast_root)))
  return copy_ast_root

def equalUnresolvedExprs(ast1: Expr, ast2: Expr):
  """
  Check if two unresolved expression trees are identical.
  "unresolved" means each node of the trees must satisfy 'schema == None'. 
  """
  ERROR_IF_NOT_INSTANCE_OF(
    ast1, Expr,
    "'ast1' is required to be instances of ast.Expr ({} received)".format(type(ast1))
  )
  ERROR_IF_NOT_INSTANCE_OF(
    ast2, Expr,
    "'ast2' is required to be instances of ast.Expr ({} received)".format(type(ast2))
  )
  # to make it simple, only the schemas of the two roots are checked here
  ERROR_IF_FALSE(
    hasattr(ast1, 'schema') and ast1.schema is None,
    "'ast1' must be unresolved."
  )
  ERROR_IF_FALSE(
    hasattr(ast2, 'schema') and ast2.schema is None,
    "'ast2' must be unresolved."
  )
  return ast1 == ast2

def equalResolvedExprs(ast1: Expr, ast2: Expr):
  """
  Check if two resolved expression trees are identical (including the comparison between schemas).
  "resolved" means schemas of nodes are not None.
  """
  ERROR_IF_NOT_INSTANCE_OF(
    ast1, Expr,
    "'ast1' is required to be instances of ast.Expr ({} received)".format(type(ast1))
  )
  ERROR_IF_NOT_INSTANCE_OF(
    ast2, Expr,
    "'ast2' is required to be instances of ast.Expr ({} received)".format(type(ast2))
  )
  # to make it simple, only the schemas of the two roots are checked here
  ERROR_IF_FALSE(
    hasattr(ast1, 'schema') and ast1.schema is not None,
    "'ast1' must be resolved."
  )
  ERROR_IF_FALSE(
    hasattr(ast2, 'schema') and ast2.schema is not None,
    "'ast2' must be resolved."
  )
  return ast1.equal(ast2, ignore_schema=False)

def equalExprs(ast1: Expr, ast2: Expr, ignore_schema: bool = True, match_loadop_and_relation: bool = False):
  """
  Check if two expression trees (both unresolved or resolved, or one resolved and another unresolved) 
    are identical under specific conditions.
  """
  ERROR_IF_NOT_INSTANCE_OF(
    ast1, Expr,
    "'ast1' is required to be instances of ast.Expr ({} received)".format(type(ast1))
  )
  ERROR_IF_NOT_INSTANCE_OF(
    ast2, Expr,
    "'ast2' is required to be instances of ast.Expr ({} received)".format(type(ast2))
  )
  return ast1.equal(ast2, ignore_schema, match_loadop_and_relation)
  

def equalPredicate(expr: SimpleOp, expr_str: str):
  """Checks if a predicate expression tree equals the string representation"""
  ERROR_IF_NOT_INSTANCE_OF(expr, SimpleOp, 
    "method 'equalPredicate' only accepts a predicate as the first argument ({} received)".format(type(expr))
  )
  return str(expr).strip().lower().split() == expr_str.strip().lower().split()

def traverse(ast: Expr, order: str = 'dfs'):
  """
  Traverses the ast by BFS or DFS
  Note that each yielded node is a reference to the original node in the ast, instead of a copy of the node by value.
  """
  ERROR_IF_NONE(ast, "cannot traverse NoneType")
  ERROR_IF_NOT_INSTANCE_OF(ast, Expr, 
    "method 'traverse' only accepts an Expr as the first argument ({} received)".format(type(ast))
  )
  if order == 'dfs':
    if ast is not None:
      yield ast
      children = getChildren(ast)
      if len(children) > 0:
        for node in traverse(children[0], order):
          yield node
        if len(children) == 2:
          for node in traverse(children[1], order):
            yield node
  elif order == 'bfs':
    if ast is not None:
      q = queue.Queue()
      q.put(ast)
      while not q.empty():
        node = q.get()
        yield node
        for child in getChildren(node):
          q.put(child)
  else:
    raise ValueError("Unsupported traversal order: {}".format(order))

def traverseWithParent(root: Expr, parent: Expr = None, child_idx: int = 0, order: str = 'dfs'):
  """
  Traverses the ast by BFS or DFS
  
  The only difference between this method and 'traverse' is that 
    this method yields not only each node but also the reference to its parent.
  """
  ERROR_IF_NONE(root, "cannot traverse NoneType")
  ERROR_IF_NOT_INSTANCE_OF(root, Expr, 
    "method 'traverseWithParent' only accepts an Expr as the first argument ({} received)".format(type(root))
  )
  if order == 'dfs':
    if root is not None:
      yield root, parent, child_idx
      children = getChildren(root)
      if len(children) > 0:
        for node, pt, chd_idx in traverseWithParent(children[0], root, 0, order):
          yield node, pt, chd_idx
        if len(children) == 2:
          for node, pt, chd_idx in traverseWithParent(children[1], root, 1, order):
            yield node, pt, chd_idx
  elif order == 'bfs':
    if root is not None:
      q = queue.Queue()
      q.put((root, parent, 0))
      while not q.empty():
        node, pt, chd_idx = q.get()
        yield node, pt, chd_idx
        children = getChildren(node)
        for i in range(len(children)):
          q.put((children[i], node, i))
  else:
    raise ValueError("Unsupported traversal order: {}".format(order))
      
    