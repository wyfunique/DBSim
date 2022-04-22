from enum import Enum
from collections import OrderedDict
import typing
from typing import NewType, Dict, List, Type, Callable
from .extended_syntax import ExtendedSyntax
from ...utils import *
from ...ast import *

Registry = OrderedDict
Keyword = str
Name = str
Token = str
TokenList = List[Token]

TriggerFunc = str
ParserFunc = str
PredParserFunc = ParserFunc


class SQLClause(Enum):
  """
  The Enum for the three main clauses in a SQL query: SELECT, FROM, and WHERE clauses.
  These main SQL clauses are the entry points for user-plugin parsers.
  
  Note:
  (1) These entry points are only for extended parsers (which parses the query to an AST) to plug in, 
        while the interfaces of any other plugin function, 
        like extended schema-resolving functions, extended operator execution functions, etc., 
        are all defined in the concrete extended syntax subclasses (e.g., SimSelectionSyntax) 
        by the class attributes, 
        e.g., _extended_data_types_ defines the extended data types, 
        _extended_predicate_op_executors_ defines the execution functions for extended predicate operators.   
  
  (2) The "FROM" entry is temporarily turned off (by commenting it out). 
      Because FROM clause possibly includes nested queries (which normally does NOT appear in SELECT and WHERE clauses),
        it makes FROM clause less flexible than SELECT and WHERE in terms of extending.
      So we turned it off currently to avoid the mess caused by extending this clause.  
  """
  SELECT = 'select' # parsers for this clause will replace the method 'query_parser_toolbox.select_stmt.standard_select'
  #FROM = 'from' # parsers for this clause will replace the method 'query_parser_toolbox.select_stmt.standard_from'
  WHERE = 'where' # parsers for this clause will replace the method 'query_parser_toolbox.select_stmt.standard_where'

class PredExprLevel(Enum):
  OR = 'or_exp'
  AND = 'and_exp'
  COMP = 'comparison_exp'
  ADD = 'additive_exp'
  MUL = 'multiplicative_exp'
  UNARY = 'unary_exp'
  VALUE = 'value_exp', 
  VAR = 'var_exp', 
  TUPLE = 'tuple_exp', 
  FUNC = 'function_exp'

PredParserMap = Dict[PredExprLevel, PredParserFunc]

class RegEntry(object):
  """
  Each RegEntry represents a registered extended syntax. 
  """
  __slots__ = ("syntax", "clause_parsers", "entry_points", )
  def __init__(
    self, 
    syntax: Type[ExtendedSyntax], 
    clause_parsers: 'OrderedDict[SQLClause, typing.Tuple[TriggerFunc, ParserFunc]]',
    entry_points: List[Callable[[], None]]
  ) -> None:
    self.syntax = syntax
    self.clause_parsers = clause_parsers
    self.entry_points = entry_points

class RegistryUtils(object):
  @classmethod
  def groupRegistryByClause(cls, registry: Registry) -> \
      'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]':
    """
    Parameters
    -----------
    registry: the dictionary of the registry

    Return
    -----------
    A dictionary where each key is a SQLClause and the value is a dict[syntax : (trigger_function, parse_function)], 
      i.e., the key indicates the clause (select/from/where) 
      while each item in the value provides the extended syntax subclass 
      and its parser_function for parsing the clause when its trigger_function over the tokens returns True. 
    """
    group = OrderedDict()
    for name in registry:
      entry: RegEntry = registry[name]
      for clause in entry.clause_parsers:
        if clause not in group:
          group[clause] = OrderedDict()
        ERROR_IF_EXISTS_IN(
          entry.syntax, group[clause], 
          "duplicated syntax '{}' for SQL clause '{}' found in registry".format(getClassNameOfInstance(entry.syntax), clause), 
          RegistryError
        )
        group[clause][entry.syntax] = entry.clause_parsers[clause]
    return group

  @classmethod
  def getAllExtendedSyntax(cls, registry: Registry) -> List[Type[ExtendedSyntax]]:
    return [reg_entry.syntax for reg_entry in registry.values()]