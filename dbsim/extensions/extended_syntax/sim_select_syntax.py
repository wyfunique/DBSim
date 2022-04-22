from ...ast import *
from .registry_utils import *
from .extended_syntax import ExtendedSyntax
from ...utils.exceptions import SQLSyntaxError
from ... import query_parser_toolbox as toolbox 
from ...compilers import local as main_compiler
from ... import schema_interpreter  
from ... import field 

from functools import partial
from enum import Enum
from scipy.spatial.distance import cosine, euclidean

import numpy as np
import typing

Vec = np.ndarray
VecDim = 4

def toVector(list_str: str):
  return np.array(eval(list_str))

class Metric(Enum):
  EUC = 1 # euclidean distance
  COS_DIST = 2 # cosine distance
  COS_SIM = 3 # cosine similarity
  DOT = 4 # dot product

class SimSelectionOp(SelectionOp):
  __slots__ = ('relation','bool_op','schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation, bool_op, schema=None, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    super().__init__(relation, bool_op, schema, cost_factor, num_input_rows)

class ToOp(BinaryOp):
  """lhs to rhs"""
  __slots__ = ('lhs', 'rhs', 'cost_factor')
  def __init__(self, lhs, rhs, cost_factor: float = DEFAULT_COST_FACTOR * VecDim):
    self.cost_factor = cost_factor
    self.lhs = lhs
    self.rhs = rhs
  def __str__(self):
    return "{} TO {}".format(str(self.lhs), str(self.rhs))

class Vector(Const):
  __slots__ = ('const',)

  def __init__(self, vector: Vec):
    ERROR_IF_NOT_INSTANCE_OF(vector, Vec, "Vector can only be initialized with {} ({} received)".format(getClassNameOfClass(Vec), type(vector)))
    self.const = vector
    self.cost_factor = TINY_COST_FACTOR * vector.size
  def __str__(self):
    return str(self.const)
  def equal(self, other: 'Vector', ignore_schema: bool = True, match_loadop_and_relation: bool = False) -> bool:
    return np.array_equal(self.const, other.const)

def getVecDistance(v1: Vec, v2: Vec, metric: Metric = Metric.EUC) -> float:
  ERROR_IF_NOT_EQ(
    v1.shape, v2.shape, 
    "could not compute the distance between vectors with different shapes {} and {}"\
      .format(v1.shape, v2.shape)
  )
  if metric == Metric.EUC:
    return euclidean(v1, v2)
  if metric == Metric.COS_DIST:
    return cosine(v1, v2)
  if metric == Metric.COS_SIM:
    return 1 - cosine(v1, v2)
  if metric == Metric.DOT:
    return np.dot(v1, v2)

def get_to_op_executor():
  return partial(main_compiler.binary_op, getVecDistance)

def convert_values_to_vectors(df_column):
  num_detect_samples = 5
  if df_column.dtype == object and all(value.startswith('[') and value.endswith(']') for value in df_column[:num_detect_samples]):
    return df_column.apply(lambda value: toVector(value)), field.FieldType.VECTOR 
  return df_column, None

def parse_value_exp(tokens: TokenList) -> Expr:
  token = tokens[0]
  vector = []
  if token == '[':
    # parse it as a vector literal
    tokens.pop(0)
    if len(tokens) == 0:
      raise SQLSyntaxError("missing closing ']'")
    token = tokens.pop(0)
    cur_element = ""
    while token != ']':
      if not token.isdigit() and token not in (',', '.'):
        raise SQLSyntaxError("invalid symbol found in vector.")
      if token.isdigit() or token == '.':
        cur_element += token
      elif token == ',':
        try:
          cur_element = float(cur_element)
        except:
          raise SQLSyntaxError("invalid vector element: '{}'".format(cur_element))
        vector.append(cur_element)
        cur_element = ""
      if len(tokens) == 0:
        raise SQLSyntaxError("missing closing ']'")
      token = tokens.pop(0)
    if len(cur_element) > 0:
      vector.append(float(cur_element))
    return Vector(np.array(vector))
  else:
    raise ParsingFailure

def parse_multiplicative_exp(tokens: TokenList) -> Expr:
  lhs = toolbox.unary_exp(tokens)
  if len(tokens):
    Op = SimSelectionSyntax.extended_mul_level_ops.get(tokens[0])
    if Op:
      tokens.pop(0)
      rhs = toolbox.unary_exp(tokens)
      lhs = Op(lhs, rhs)
      return lhs
    else:
      raise ParsingFailure
  raise ParsingFailure

class SimSelectionSyntax(ExtendedSyntax):
  """
  Example #1:

    simselect employee_id
    from employees
    where employees.vector to [1.0,2.0,1.2,5.1] < 1.5
  
  Example #2:

    simselect employees.vector to [1.0,2.0,1.2,5.1]
    from employees
  """
  simselect_keyword = 'simselect'
  extended_mul_level_ops = {'to': ToOp}

  _extended_symbols_: str = '[]'
  _extended_clause_keywords_ = {SQLClause.SELECT: simselect_keyword}
  _extended_data_types_: typing.Dict[typing.Type[Const], str] = {Vector: 'VECTOR'}
  _extended_data_types_converter_ = convert_values_to_vectors
  _extended_predicate_parsers_ = ({
    PredExprLevel.MUL: parse_multiplicative_exp,
    PredExprLevel.VALUE: parse_value_exp
  }, False)
  _extended_predicate_op_executors_: \
      typing.Dict[typing.Type[SimpleOp], typing.Callable[[typing.Any], typing.Callable]] = \
        {ToOp: get_to_op_executor()}
  _extended_relation_op_schema_: \
      typing.Dict[typing.Type[SuperRelationalOp], typing.Callable[[SuperRelationalOp, 'DataSet'], 'Schema']] = \
        {SimSelectionOp: schema_interpreter.schema_from_relation}
  _extended_relation_op_executors_: \
      typing.Dict[typing.Type[SuperRelationalOp], typing.Callable[[typing.Any], typing.Callable]] = \
        {SimSelectionOp: main_compiler.selection_op}

  
  def __init__(self):
    self.simselect_detected: bool = False
    """
    A flag for whether the query is a standard select query or a simselect query.
    This is used by parse_simselect_where to determine whether to throw an error when keyword 'to' is not met.
    
    Specifically, when 'simselect' is detected, there must be at least one 'to' 
      existing in either the SELECT cluase or the WHERE clause of the current query, 
      otherwise a syntax error should be thrown. 
    """
    self.to_keyword_detected_in_select_clause: bool = False
    """
    A flag for whether 'to' keyword is found in the SELECT clause of the current query.
    If it is found, a SimSelectionOp instead of SelectionOp will be generated, 
      whether 'simselect' is used or not.
    If it is not found in SELECT clause, it will be checked in WHERE clause later, 
      and if found there, a SimSelectionOp will be generated.
    Only when there is no 'to' and no 'simselect' keywords detected in both of SELECT and WHERE clauses, 
      the query will be parsed to a standard selection (SelectionOp).  
    
    Note that the FROM clause will not be checked for 'to' 
      since 'to' is a predicate operator (i.e., should only present in predicate expressions) 
      instead of a relational operator like join. 
    So if 'to' is found in FROM clause, it must be in a nested query, 
      which has no impact on the parsing of the current query level. 
    """
    self.I_am_triggered: bool = False
    """
    Flag to tell 'trigger_simselect_where' if 'trigger_simselect' returns True.
    If yes, 'trigger_simselect_where' will always return True.
    Otherwise 'trigger_simselect_where' will do its own checking and return True or False.
    """

  def trigger_simselect(self, tokens: TokenList) -> bool:
    try:
      index_of_from_clause = tokens.index('from')
      # 'from' exists in tokens, there may be nested queries.
      # In such case only needs to check the tokens before 'from', 
      #   and the nested queries will be checked later 
      #   during the recursive parsing for the inner select statements.
      self.I_am_triggered = \
        tokens[0] == self.simselect_keyword \
        or (tokens[0] == 'select' and 'to' in tokens[:index_of_from_clause])
      return self.I_am_triggered
    except ValueError:
      # no 'from' in tokens, i.e., there are no nested queries
      self.I_am_triggered = \
        tokens[0] == self.simselect_keyword \
        or (tokens[0] == 'select' and 'to' in tokens)
      return self.I_am_triggered
    except Exception as e:
      raise e

  def parse_simselect(self, tokens: TokenList) -> List[Expr]:
    # resets the flags to avoid the influence from previous queries 
    self.simselect_detected = False
    self.to_keyword_detected_in_select_clause = False

    if tokens[0] not in ('select', self.simselect_keyword):
      raise SQLSyntaxError
    keyword = tokens.pop(0)
    if keyword == self.simselect_keyword:
      # 'simselect' detected
      self.simselect_detected = True
    for token in tokens:
      if token in ('from', 'where'):
        # the current SELECT clause ends at 'from' or 'where' or the tail of tokens
        break
      if token == 'to':
        # 'to' detected in the current SELECT clause
        self.to_keyword_detected_in_select_clause = True
    
    # Whether the extended keywords 'simselect' and 'to' are found or not, 
    #   simply does the same thing as standard select, 
    #   i.e., using select_core_exp to parse the columns to be selected  
    select_cols = toolbox.select_core_exp(tokens) 
    return select_cols 
  
  def trigger_simselect_where(self, tokens: TokenList) -> bool:
    return self.I_am_triggered or (tokens[:1] == ['where'] and 'to' in tokens)

  def parse_simselect_where(self, tokens: TokenList, relation: Expr) -> Expr:
    if tokens[:1] == ['where']:
      tokens.pop(0)
      # validate the syntax requirements on the occurrance of 'simselect' and 'to'
      to_keyword_detected_in_where_clause = False
      for token in tokens:
        if token in ('select', 'from', 'where', self.simselect_keyword):
          # If any one among the 'select', 'from', 'where' and 'simselect' keywords is met, 
          #   it means the current WHERE clause is part of a nested query, 
          #   and the current WHERE clause reaches at most the token right before the met keyword,
          #   i.e., the met keyword can be used as a rough boundary of the current WHERE clause
          #   for the purpose of checking occurrance of 'to'.
          break
        if token == 'to':
          # 'to' detected in the current WHERE clause
          to_keyword_detected_in_where_clause = True
      if self.simselect_detected:
        ERROR_IF_FALSE(
          self.to_keyword_detected_in_select_clause or to_keyword_detected_in_where_clause, 
          "simselect requires at least one 'to' expression, zero found.", 
          ExtendedSyntaxError
        )
      if to_keyword_detected_in_where_clause or self.to_keyword_detected_in_select_clause or self.simselect_detected: 
        # if 'to' keyword is found in WHERE clause or in SELECT clause, 
        #   or the keyword 'simselect' instead of 'select' is used in SELECT clause, 
        #   calls extended_where_core_expr here to generate a SimSelectionOp
        relation: SimSelectionOp = self.extended_where_core_expr(tokens, relation)
      else:
        # otherwise, calls the standard where_core_expr to generate a standard SelectionOp
        relation: SelectionOp = toolbox.where_core_expr(tokens, relation)
    else:
      # The current query has no WHERE clause,  
      #   but we still need to validate the syntax requirements 
      #   on the occurrance of 'simselect' and 'to'.
      if self.simselect_detected:
        ERROR_IF_FALSE(
          self.to_keyword_detected_in_select_clause,
          "simselect requires at least one 'to' expression, zero found.", 
          ExtendedSyntaxError
        )

    # resets the flags to avoid affecting the parsing of future queries 
    #self.simselect_detected = False
    #self.to_keyword_detected_in_select_clause = False
    return relation

  def extended_where_core_expr(self, tokens: TokenList, relation: Expr) -> SimSelectionOp:
    return SimSelectionOp(relation, toolbox.or_exp(tokens))

 
