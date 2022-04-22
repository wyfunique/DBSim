from . import Field
from . import Schema
from .ast import *
from .utils import * 
from .utils.exceptions import SQLSyntaxError
from .query_parser_toolbox import *
from .extensions.extended_syntax.registry import getRegistry
from .extensions.extended_syntax.registry_utils import *

# Parses a SQL query string and returns an AST
def parse(statement, root_exp = None):
  term = set(terminators)

  if root_exp is None:
    root_exp = or_exp 

  tokens = [  
    token.lower() if token.lower() in term else token
    for token in ExtensibleTokens(statement) 
  ]

  extended_syntax_registry: 'OrderedDict[Name, RegEntry]' = getRegistry()
  clauses_to_parsers: \
      'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' =\
    RegistryUtils.groupRegistryByClause(extended_syntax_registry)
  exp = root_exp(tokens, clauses_to_parsers)
  if tokens: 
    raise SyntaxError('Incomplete statement {}'.format(tokens))
  return exp

def parse_statement(statement):
  return parse(statement, root_exp=union_stmt)

def parse_select(relation, statement):
  columns = parse(
    statement, 
    root_exp=lambda tokens: select_core_exp(tokens)
  )
  return projection_op(relation, columns)

def parse_order_by(statement):
  return parse(
    statement, 
    root_exp=order_by_core_expr
  )

def parse_group_by(statement):
  return parse(
    statement, 
    root_exp=group_by_core_expr
  )
