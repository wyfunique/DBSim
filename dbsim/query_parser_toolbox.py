import string
import typing

import codd

from . import Field
from . import Schema
from .ast import *
from .utils import * 
from .utils.exceptions import *
from .utils.logger import Logger
from .extensions.extended_syntax.extended_syntax import ExtendedSyntax
from .extensions.extended_syntax.registry_utils import *

logger = Logger.general_logger
BLOCK_ERROR = True

def syntaxFuncFromName(syntax_instance: ExtendedSyntax, func_name: str) -> Callable:
  return getFuncByName(syntax_instance, func_name)

def addSyntaxSymbol(new_symbols: str) -> int:
  count_success = 0
  for symbol in new_symbols:
    if symbol not in ExtensibleTokens.SYMBOLS:
      ExtensibleTokens.SYMBOLS += symbol
      count_success += 1
  return count_success

def removeSyntaxSymbol(symbols: str) -> int:
  count_success = 0
  for symbol in symbols:
    symbol_idx = ExtensibleTokens.SYMBOLS.find(symbol)
    if symbol_idx >= 0:
      ExtensibleTokens.SYMBOLS = ExtensibleTokens.SYMBOLS[:symbol_idx] + ExtensibleTokens.SYMBOLS[symbol_idx+1:]
      count_success += 1
  return count_success

def addClauseKeywords(keywords: Dict[SQLClause, str]):
  global sql_clause_keywords, terminators
  for clause in keywords:
    ERROR_IF_NOT_INSTANCE_OF(clause, SQLClause, 
      "'{}' is not a {} instance.".format(type(clause), getClassNameOfClass(SQLClause))
    )
    ERROR_IF_FALSE(clause in sql_clause_keywords,
      "Attempted to add keywords into non-supported SQL clause '{}'.".format(type(clause)), 
      ExtensionInternalError
    )
    ERROR_IF_NOT_INSTANCE_OF(keywords[clause], str, 
      "'{}' is not a {}.".format(type(keywords[clause]), getClassNameOfClass(str))
    )
    sql_clause_keywords[clause].add(keywords[clause])
    terminators = tuple(list(terminators) + [keywords[clause]])

def removeClauseKeywords(keywords: Dict[SQLClause, str]):
  global sql_clause_keywords, terminators
  for clause in keywords:
    if clause in sql_clause_keywords:
      sql_clause_keywords[clause].remove(keywords[clause])
      terminators = list(terminators)
      terminators.remove(keywords[clause])
      terminators = tuple(terminators)

class ExtensibleTokens(codd.Tokens):
  
  SYMBOLS = codd.SYMBOLS 

  def read_symbol(self):
    if self.current_char in ExtensibleTokens.SYMBOLS:
      char = self.current_char
      self.read_char()
      return char
    elif self.current_char in "?$":
      char = self.current_char
      self.read_char()
      return char+self.read_word()

    elif self.current_char == '<':
      self.read_char()
      if self.current_char == '=':
        self.read_char()
        return '<='
      else:
        return '<'
    elif self.current_char == '>':
      self.read_char()
      if self.current_char == '=':
        self.read_char()
        return '>='
      else:
        return '>'
    elif self.current_char == '!':
      self.read_char()
      if self.current_char == '=':
        self.read_char()
        return "!="
      else:
        return '!'
    else:
      raise RuntimeError("Unexpected token " + self.current_char)

def addPredParsers(syntax: Type[ExtendedSyntax], 
    parsers: Dict[PredExprLevel, Callable], block_error: bool = not BLOCK_ERROR):
  global predicate_parsers
  for level in parsers:
    ERROR_IF_NOT_INSTANCE_OF(level, PredExprLevel, 
      "'{}' is not a {} instance.".format(type(level), getClassNameOfClass(PredExprLevel))
    )
    ERROR_IF_FALSE(level in predicate_parsers,
      "Adding predicate parsers of new predicate levels is not supported currently.", 
      ExtensionInternalError
    )
    predicate_parsers[level].add(getClassNameOfClass(syntax), parsers[level], block_error)

def resetPredParsers():
  global predicate_parsers
  predicate_parsers = {
    PredExprLevel.OR: ParsersBundle([('StandardSyntax', or_exp, not BLOCK_ERROR)], PredExprLevel.OR),
    PredExprLevel.AND: ParsersBundle([('StandardSyntax', and_exp, not BLOCK_ERROR)], PredExprLevel.AND),
    PredExprLevel.COMP: ParsersBundle([('StandardSyntax', comparison_exp, not BLOCK_ERROR)], PredExprLevel.COMP),
    PredExprLevel.ADD: ParsersBundle([('StandardSyntax', additive_exp, not BLOCK_ERROR)], PredExprLevel.ADD),
    PredExprLevel.MUL: ParsersBundle([('StandardSyntax', multiplicative_exp, not BLOCK_ERROR)], PredExprLevel.MUL),
    PredExprLevel.UNARY: ParsersBundle([('StandardSyntax', unary_exp, not BLOCK_ERROR)], PredExprLevel.UNARY),
    PredExprLevel.VALUE: ParsersBundle([('StandardSyntax', value_exp, not BLOCK_ERROR)], PredExprLevel.VALUE),
    PredExprLevel.VAR: ParsersBundle([('StandardSyntax', var_exp, not BLOCK_ERROR)], PredExprLevel.VAR),
    PredExprLevel.TUPLE: ParsersBundle([('StandardSyntax', tuple_exp, not BLOCK_ERROR)], PredExprLevel.TUPLE),
    PredExprLevel.FUNC: ParsersBundle([('StandardSyntax', function_exp, not BLOCK_ERROR)], PredExprLevel.FUNC)
  }

class ParsersBundle:
  """
  The suite collecting multiple parsers for a PredExprLevel. 
  Each time there are tokens to be parsed on a level, 
  this suite will try all the included parsers until finding the one 
    which can parse the tokens without error, and return the parsed results.

  This class is designed to allow the parsers from multiple ExtendedSyntax to co-exist.
  """
  __slots__ = ("parsers", "predicate_level")
  def __init__(self, parsers: List[typing.Tuple[Callable[[TokenList], Expr], bool]], level: PredExprLevel):
    self.predicate_level = level
    self.parsers = parsers
  
  def __call__(self, arg1, arg2 = None) -> Expr:
    if self.predicate_level in (PredExprLevel.FUNC, PredExprLevel.VAR):
      name = arg1
      tokens = arg2
    else:
      tokens = arg1
    tokens_copy = deepcopy(tokens)
    res = None
    for idx, (syntax_name, parser, block_error) in enumerate(self.parsers):
      try:
        if self.predicate_level in (PredExprLevel.FUNC, PredExprLevel.VAR):
          res = parser(name, tokens_copy)
        else:
          res = parser(tokens_copy)
        break
      except Exception as e:
        """
        if block errors, all exceptions including ParsingFailure will not be raised;
        if not block, any exception excluding ParsingFailure will be raised.
        i.e., as an informational exception, ParsingFailure will never interrupt the parsing process. 
        """
        if not block_error:
          if not isinstance(e, ParsingFailure):
            logger.error("{} raised an error: (see the info below)".format(syntax_name))
            raise e
          # if the error is ParsingFailure, meaning the current parser cannot parse the tokens, 
          #   at which time the parsing work is simply passed to the next parser, 
          #   and no exception will be raised.
        tokens_copy = deepcopy(tokens)
        continue
    ERROR_IF_NONE(res, "could not parse tokens {} by any parser of level {}".format(tokens, self.predicate_level), SQLSyntaxError)
    # make tokens the same as tokens_copy
    #   such that the next parsers can continue from the latest status of tokens
    if self.predicate_level in (PredExprLevel.FUNC, PredExprLevel.VAR):
      parser(name, tokens)
    else:
      parser(tokens)
    return res

  def add(self, syntax_name: str, parser: Callable[[TokenList], Expr], block_error: bool = not BLOCK_ERROR):
    if len(self.parsers) == 1:
      # when there is only the standard parser, 
      #   prepend the extended parser before it.
      self.parsers = [(syntax_name, parser, block_error)] + self.parsers
    else:
      # when there are already other extended parsers, 
      #   insert the new extended parser after all those existing extended parsers
      #   but still before the standard parser.
      # i.e., make sure the extended parsers are tried in order by the registry, 
      #   and always place the standard parser the last one to try. 
      self.parsers = self.parsers[:-1] + [(syntax_name, parser, block_error)] + [self.parsers[-1]] 

######## parsing routines #################### 
#   Note: the following functions  
#   were moved here from query_parser.py
#   to avoid circular importing issue.
##############################################   

def or_exp(tokens):
  parse_and = predicate_parsers[PredExprLevel.AND]
  lhs = parse_and(tokens)
  while len(tokens) and tokens[0] == 'or':
    tokens.pop(0)
    lhs = Or(lhs, parse_and(tokens))
  return lhs
  
def and_exp(tokens):
  parse_comparison = predicate_parsers[PredExprLevel.COMP]
  lhs = parse_comparison(tokens)
  while len(tokens) and tokens[0] == 'and':
    tokens.pop(0)
    lhs = And(lhs, parse_comparison(tokens))
  return lhs

def comparison_exp(tokens):
  parse_additive = predicate_parsers[PredExprLevel.ADD]
  parse_tuple = predicate_parsers[PredExprLevel.TUPLE]
  lhs = parse_additive(tokens)
  if len(tokens):
    
    if [t.lower() for t in tokens[0:2]] == ['not','like']:
      tokens.pop(0)
      tokens.pop(0)
      tokens.insert(0, 'not like')

    if [t.lower() for t in tokens[0:2]] == ['not','rlike']:
      tokens.pop(0)
      tokens.pop(0)
      tokens.insert(0, 'not rlike')


    if tokens[0] == 'between':
      tokens.pop(0)
      expr = lhs 
      lhs = comparison_exp(tokens)
      if tokens[0] != 'and':
        raise SyntaxError("missing 'AND' ")
      tokens.pop(0)
      rhs = comparison_exp(tokens)
      return BetweenOp(expr, lhs, rhs)


    elif tokens[0:2] == ['in', '(']:

      tokens.pop(0)
      tokens.pop(0)
      return InOp(lhs, parse_tuple(tokens))

    elif tokens[0:3] == ['not','in', '(']:

      tokens.pop(0)
      tokens.pop(0)
      tokens.pop(0)
      return NotOp(InOp(lhs, parse_tuple(tokens)))



    elif tokens[0].lower() in COMPARISON_OPS:
      token = tokens.pop(0)
      if tokens and tokens[0] == 'not':

        token = 'is not'
        tokens.pop(0)

      Op = COMPARISON_OPS[token.lower()]
      rhs =  parse_additive(tokens)
      return Op(lhs, rhs)

  # otherwise
  return lhs

def additive_exp(tokens):
  parse_multiplicative = predicate_parsers[PredExprLevel.MUL]
  lhs = parse_multiplicative(tokens)
  while tokens:
    Op = ADDITIVE_OPS.get(tokens[0])
    if Op:
      tokens.pop(0)
      rhs = parse_multiplicative(tokens)
      lhs = Op(lhs, rhs)
    else:
      break
  return lhs

def multiplicative_exp(tokens):
  parse_unary = predicate_parsers[PredExprLevel.UNARY]
  lhs = parse_unary(tokens)
  while len(tokens):
    Op = MULTIPLICATIVE_OPS.get(tokens[0])
    if Op:
      tokens.pop(0)
      rhs = parse_unary(tokens)
      lhs = Op(lhs, rhs)
    else:
      break
  return lhs

def unary_exp(tokens):
  parse_value = predicate_parsers[PredExprLevel.VALUE]
  if tokens[0] == '-':
    tokens.pop(0)
    value = parse_value(tokens)
    return NegOp(value)
  elif tokens[0] == 'not':
    tokens.pop(0)
    value = parse_value(tokens)
    return NotOp(value)
  elif tokens[0] == '+':
    tokens.pop(0)
    
  return parse_value(tokens)

def value_exp(tokens):
  """
  Returns a function that will return a value for the given token
  """
  token = tokens.pop(0)
  
  parse_tuple = predicate_parsers[PredExprLevel.TUPLE]
  parse_function = predicate_parsers[PredExprLevel.FUNC]
  parse_var = predicate_parsers[PredExprLevel.VAR]
  # todo: consider removing select $0 and instead
  # requiring table table.field[0]

  if token.startswith('$'):
    key = token[1:]
    try:
      key = int(key)
    except ValueError:
      pass

    return ItemGetterOp(key)

  if token.startswith('?'):
    pos = int(token[1:])
    return ParamGetterOp(pos)
  elif token == 'null':
    return NullConst()
  elif token[0] in string.digits:
    if tokens and tokens[0] == '.':
      value = float(token + tokens.pop(0) + tokens.pop(0))
    else:
      value = int(token)
    return NumberConst(value)
  elif token[0] in ("'",'"'):
    return StringConst(token[1:-1])
  elif token == '(':
    return parse_tuple(tokens)
  elif token.lower() == 'case':
    return case_when_core_exp(tokens)
  elif token.lower() == 'cast':
    return cast_core_exp(tokens)
  #elif token in SYMBOLS: 
  #  return lambda row, ctx: token
  else:

    if tokens and tokens[0] == '(':
      return parse_function(token, tokens)
    else:
      return parse_var(token, tokens)

def tuple_exp(tokens):
  args = []
  parse_or = predicate_parsers[PredExprLevel.OR]
  if tokens and tokens[0] != ')':
    args.append(parse_or(tokens))
    while tokens[0] == ',':
      tokens.pop(0)
      args.append(parse_or(tokens))
  if not tokens or tokens[0] != ')':
    raise SyntaxError("missing closing ')'")
  
  tokens.pop(0)

  return Tuple(*args)
 

def function_exp(name, tokens):
  token = tokens.pop(0)
  if token != '(':
    raise SyntaxError('Expecting "("')

  parse_tuple = predicate_parsers[PredExprLevel.TUPLE]
  args = parse_tuple(tokens)
  return Function(name, *args.exprs)


def cast_core_exp(tokens):
  token = tokens.pop(0)
  if token != '(':
    raise SyntaxError('Expected "("')
  parse_or = predicate_parsers[PredExprLevel.OR]
  expr = parse_or(tokens)
  token = tokens.pop(0)
  if token.lower() != 'as':
    raise SyntaxError('Expected "AS"')
  if tokens[1] != ')':
    type = parse_or(tokens)
  else:
    type = tokens.pop(0)
  token = tokens.pop(0)
  if token != ')':
    raise SyntaxError('Expected ")"')
  return CastOp(expr, type)


def case_when_core_exp(tokens):
  all_conditions = []
  parse_or = predicate_parsers[PredExprLevel.OR]
  if  tokens[0].lower()  != 'when':
    raise SyntaxError('Expected "WHEN"')
  while tokens and  tokens[0].lower() == 'when':
    token = tokens.pop(0).lower()
    condition = parse_or(tokens)
    token = tokens.pop(0).lower()
    if token != 'then':
      raise SyntaxError('Expected "THEN"')
    expr = parse_or(tokens)
    condition_map = dict(
      condition=condition,
      expr=expr
    )
    all_conditions.append(condition_map)
  
  if tokens[0].lower() == 'else':
    tokens.pop(0)
    def_value = parse_or(tokens)
  else:
    def_value = None
  token=tokens.pop(0).lower()
  if token != 'end':
    raise SyntaxError('Expected "END"')
  return CaseWhenOp(all_conditions, def_value)

reserved_words = ['is','in']
def var_exp(name, tokens, allowed=string.ascii_letters + '_'):
  if name in reserved_words:
    raise SyntaxError('invalid syntax')
  path = [name]

  while len(tokens) >= 2 and tokens[0] == '.' and tokens[1][0] in allowed:
    tokens.pop(0) # '.'
    path.append(tokens.pop(0)) 

  return Var('.'.join(path)) 


# sql specific parsing
terminators = ('from',
 'where',
 'limit',
 'offset',
 'having',
 'group',
 'by',
 'order',
 'left',
 'join',
 'on',
 'union',
 'outer',

'in',
'is',
'and',
'or',
'select',
'between',
'not',

')'
 )

def projection_op(relation,
 columns):
  if len(columns) == 1 and isinstance(columns[0], SelectAllExpr) and columns[0].table is None:
    # select all columns, i.e., 'select *'
    return relation
  else:
    return ProjectionOp(relation, *columns)

def select_core_exp(tokens) -> List[Expr]:
  """
  Parses the columns to be selected (which are between 'select' and 'from'/'where' keywords) 
    and returns the corresponding operators over them 
  """
  columns = []

  while tokens and tokens[0] not in terminators:
    col = result_column_exp(tokens)

    columns.append(col)
    if tokens and tokens[0] == ',':
      tokens.pop(0)

  return columns

def join_source(
  tokens,
  clauses_to_parsers: \
    'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
    = OrderedDict()
):
  """
  Parses and returns the data sources (i.e., the stuffs between 'from' and 'where' keywords in SQL)
  """
  # Always parses at least one data source
  source = single_source(tokens, clauses_to_parsers)
  # If the current SQL query includes Join, continues parsing the other data sources
  while tokens and tokens[0] in (',', 'join', 'left'):

    join_type = tokens.pop(0)

    if join_type == 'left':
      if tokens[0] == 'outer':
        tokens.pop(0)
      ERROR_IF_NOT_EQ(tokens[0], 'join', "Missing keyword 'join' after 'left'", SQLSyntaxError)
      tokens.pop(0)
      op = LeftJoinOp
    else:
      op = JoinOp

    right = single_source(tokens, clauses_to_parsers)
    if tokens and tokens[0] == 'on':
      tokens.pop(0)
      parse_or = predicate_parsers[PredExprLevel.OR]
      source = op(source, right, parse_or(tokens))
    else:
      source = op(source, right)

  return source

def single_source(
  tokens, 
  clauses_to_parsers: \
    'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
    = OrderedDict()
):
  """
  Parses and returns the first data source from the tokens
  """
  if tokens[0] == '(':
    # The first data source is a nested SQL query
    tokens.pop(0)
    if tokens[0] in sql_clause_keywords[SQLClause.SELECT]:
      source = select_stmt(tokens, clauses_to_parsers)
    else:
      source = join_source(tokens, clauses_to_parsers)

    if tokens[0] != ')':
      raise SyntaxError('Expected ")"')
    else:
      tokens.pop(0)


    if tokens and tokens[0] not in ',' and tokens[0] not in terminators:
      if tokens[0] == 'as':
        tokens.pop(0)
      alias = tokens.pop(0)
      source = AliasOp(alias, source)

    return source
  else:
    # The first data source is not a complete SQL query but a simple table name, function, or other stuffs.
    if tokens[1:2] == ['(']:
      # Current tokens start with a string followed by a '(', i.e., a function
      source = relation_function_exp(tokens.pop(0), tokens, clauses_to_parsers)
    else:
      if tokens[:2][-1] == '.':
        # Current tokens start with a full table name that looks like 'schemaName.tableName'
        name = tokens.pop(0) + tokens.pop(0) + tokens.pop(0)
      else:
        # start with a simple table name without schema name
        name = tokens.pop(0)
      # the data source is a table, load it
      source = LoadOp(name)

    if tokens and tokens[0] not in ',' and tokens[0] not in terminators:
      if tokens[0] == 'as':
        tokens.pop(0)
      alias = tokens.pop(0)
      source = AliasOp(alias, source)
    return source


def relation_function_exp(
  name, 
  tokens, 
  clauses_to_parsers: \
    'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
    = OrderedDict()
):

  token = tokens.pop(0)
  if token != '(':
    raise SyntaxError("Expecting '('")

  args = []

  while tokens and tokens[0] != ")":
    if tokens[0] == '(':


      args.append(single_source(tokens, clauses_to_parsers))
    else:
      parse_value = predicate_parsers[PredExprLevel.VALUE]
      expr = parse_value(tokens)
      if isinstance(expr, Var):
        args.append(LoadOp(expr.path))
      elif isinstance(expr, Const):
        args.append(expr)
      else:
        raise ValueError("Only constants, relationame or select queries allowed")

    if tokens[0] == ',':
      tokens.pop(0)

 
  if tokens[0] != ')':
    raise SyntaxError("Expecting ')'")
  else:
    tokens.pop(0)

  return Function(name, *args)


def result_column_exp(tokens) -> Expr:

  if tokens[0] == '*':
    tokens.pop(0)
    return SelectAllExpr()
  else:
    parse_or = predicate_parsers[PredExprLevel.OR]
    exp = parse_or(tokens)
    if tokens and isinstance(exp, Var) and tokens[:2] == ['.','*']:
      tokens.pop(0) # '.'
      tokens.pop(0) # '*'
      return SelectAllExpr(exp.path)
    else:
      if tokens and tokens[0].lower() == 'as':
        tokens.pop(0) # 'as'
        alias = tokens.pop(0)
        return RenameOp(alias, exp)
      else:
        return exp


def where_core_expr(tokens, relation):
  parse_or = predicate_parsers[PredExprLevel.OR]
  return SelectionOp(relation, parse_or(tokens))

def order_by_core_expr(tokens):
  columns = []

  parse_value = predicate_parsers[PredExprLevel.VALUE]
  while tokens and tokens[0] not in terminators:
    col = parse_value(tokens)
    if tokens: 
      if tokens[0].lower() == "desc":
        col = Desc(col)
        tokens.pop(0)
      elif tokens[0].lower() == "asc":
        tokens.pop(0)

    columns.append(col)

    if tokens and tokens[0] == ',':
      tokens.pop(0)


  return columns

def group_by_core_expr(tokens):

  columns = []
  parse_var = predicate_parsers[PredExprLevel.VAR]
  while tokens and tokens[0] not in terminators:
    token = tokens.pop(0)
    columns.append(parse_var(token, tokens))
    if tokens and tokens[0] == ',':
      tokens.pop(0)

  return columns

def union_stmt(
    tokens: TokenList, 
    clauses_to_parsers: \
      'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
      = OrderedDict()
) -> Expr:

  op = select_stmt(tokens, clauses_to_parsers)

  if not tokens:
    return op
  elif tokens[0:2] == ["union", "all"]:
    tokens.pop(0)
    tokens.pop(0)
    return UnionAllOp(op, union_stmt(tokens, clauses_to_parsers))
  else:
    raise SyntaxError('Incomplete statement {}'.format(tokens))


def select_stmt(
    tokens: TokenList, 
    clauses_to_parsers: \
      'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
      = OrderedDict()
) -> Expr:

  def standard_select(tokens: TokenList) -> List[Expr]:
    if tokens[0] != 'select':
      raise SyntaxError
    tokens.pop(0)
    # select_core_exp returns operators over the columns to be selected  
    select_cols = select_core_exp(tokens) 
    return select_cols 

  def standard_from(
    tokens: TokenList, 
    clauses_to_parsers: \
      'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
      = OrderedDict()
  ) -> Expr:
    if tokens and tokens[0] == 'from' :
      tokens.pop(0)
      # join_source: parses and returns the data sources (i.e., the stuffs between 'from' and 'where' keywords in SQL)
      #   For single data source, it returns the source; 
      #   while for multiple data sources (i.e., join operation), it returns a LeftJoinOp, JoinOp or AliasOp over the sources.
      relation = join_source(tokens, clauses_to_parsers) #from_core_exp(tokens)
    else:
      relation = LoadOp('')
    return relation

  def standard_where(tokens: TokenList, relation: Expr) -> Expr:
    if len(tokens) > 0 and tokens[0] in sql_clause_keywords[SQLClause.WHERE]:
      tokens.pop(0)
      relation = where_core_expr(tokens, relation)
    return relation

  parse_select_by_standard, parse_from_by_standard, parse_where_by_standard = True, True, True
  syntax_instances: 'OrderedDict[Type[ExtendedSyntax], ExtendedSyntax]' = OrderedDict()
  for ordered_dict in clauses_to_parsers.values():
    # prepare all the needed syntax instances to be used later
    for syntax in ordered_dict:
      syntax_instances[syntax] = syntax()
  
  triggered_clause_parsers_for_select: 'OrderedDict[SQLClause, typing.Tuple[Type[ExtendedSyntax], ParserFunc]]' = \
    getTriggeredSQLClauseParsersInfo(tokens, syntax_instances, clauses_to_parsers, (SQLClause.SELECT, ))
  
  if len(triggered_clause_parsers_for_select) > 0:
    syntax = triggered_clause_parsers_for_select[SQLClause.SELECT][0]
    parser_func_name = triggered_clause_parsers_for_select[SQLClause.SELECT][1]
    parse_syntax = syntaxFuncFromName(
        syntax_instances[syntax], parser_func_name
    )
    select_cols: List[Expr] = parse_syntax(tokens)   
    parse_select_by_standard = False
  if parse_select_by_standard:
    # Either there is no extended syntax defined on SELECT clause, 
    #   or no syntax is currently triggered on SELECT clause, 
    #   in which cases the standard syntax is used to parse the SELECT clause.
    select_cols: List[Expr] = standard_select(tokens)

  if parse_from_by_standard:
    # Either there is no extended syntax defined on FROM clause, 
    #   or no syntax is currently triggered on FROM clause, 
    #   in which cases the standard syntax is used to parse the FROM clause.
    relation: Expr = standard_from(tokens, clauses_to_parsers)
  
  triggered_clause_parsers_for_where: 'OrderedDict[SQLClause, typing.Tuple[Type[ExtendedSyntax], ParserFunc]]' = \
    getTriggeredSQLClauseParsersInfo(tokens, syntax_instances, clauses_to_parsers, (SQLClause.WHERE, ))
  if len(triggered_clause_parsers_for_where) > 0:
    syntax = triggered_clause_parsers_for_where[SQLClause.WHERE][0]
    parser_func_name = triggered_clause_parsers_for_where[SQLClause.WHERE][1]
    parse_syntax = syntaxFuncFromName(
        syntax_instances[syntax], parser_func_name
    )
    relation: Expr = parse_syntax(tokens, relation)   
    parse_where_by_standard = False
  if parse_where_by_standard:
    # Either there is no extended syntax defined on WHERE clause, 
    #   or no syntax is currently triggered on WHERE clause, 
    #   in which cases the standard syntax is used to parse the WHERE clause.
    relation: Expr = standard_where(tokens, relation)


  relation =  projection_op(relation, select_cols)


  if tokens[:2] == ['group', 'by']:
    
    tokens.pop(0)
    tokens.pop(0)
  
    relation = GroupByOp(relation, *group_by_core_expr(tokens))


  if tokens[:2] == ['order', 'by']:
    tokens.pop(0)
    tokens.pop(0)
    relation = OrderByOp(relation, *order_by_core_expr(tokens))

  start = stop = None
  parse_value = predicate_parsers[PredExprLevel.VALUE]
  if tokens and tokens[0] =='limit':
    tokens.pop(0)
    stop = parse_value(tokens).const

  if tokens and tokens[0] == 'offset':
    start = parse_value(tokens).const
    if stop is not None:
      stop += start

  if not( start is None and stop is None):
    relation = SliceOp(relation, start, stop)
    
  return relation

def getTriggeredSQLClauseParsersInfo(
  tokens: TokenList, 
  syntax_instances: 'OrderedDict[Type[ExtendedSyntax], ExtendedSyntax]', 
  clauses_to_parsers: \
    'OrderedDict[SQLClause, OrderedDict[Type[ExtendedSyntax], typing.Tuple[TriggerFunc, ParserFunc]]]' \
    = OrderedDict(), 
  target_clauses: typing.Tuple[SQLClause] = (SQLClause.SELECT, SQLClause.WHERE)
) -> 'OrderedDict[SQLClause, typing.Tuple[Type[ExtendedSyntax], ParserFunc]]':
  """Returns dict(SQLCLause -> (the_class_of_syntax, name_of_the_triggered_clause_parser_from_this_syntax))"""
  clause_parsers_triggered: 'OrderedDict[SQLClause, typing.Tuple[Type[ExtendedSyntax], ParserFunc]]' = OrderedDict()
  target_clauses = set(target_clauses)
  for sql_clause in clauses_to_parsers:
    if sql_clause not in target_clauses:
      continue
    for syntax in clauses_to_parsers[sql_clause]:
      is_triggered = syntaxFuncFromName(syntax_instances[syntax], clauses_to_parsers[sql_clause][syntax][0])
      if is_triggered(tokens) == True:
        # the clause parser of this syntax is triggered for this SQLClause 
        if sql_clause in clause_parsers_triggered:
          # There is already another triggered clause parser for this SQLClause, 
          #   in which case the current clause parser is ignored and print a warning about that.
          logger.warn(
            "Multiple clause parsers are triggered for the same SQL clause ({} parsers of {} and {})"\
            .format(
              sql_clause,
              getClassNameOfClass(clause_parsers_triggered[sql_clause][0]),
              getClassNameOfClass(syntax)
            )
          )
        else:
          clause_parsers_triggered[sql_clause] = (syntax, clauses_to_parsers[sql_clause][syntax][1])
  return clause_parsers_triggered


predicate_parsers = {
  PredExprLevel.OR: ParsersBundle([('StandardSyntax', or_exp, not BLOCK_ERROR)], PredExprLevel.OR),
  PredExprLevel.AND: ParsersBundle([('StandardSyntax', and_exp, not BLOCK_ERROR)], PredExprLevel.AND),
  PredExprLevel.COMP: ParsersBundle([('StandardSyntax', comparison_exp, not BLOCK_ERROR)], PredExprLevel.COMP),
  PredExprLevel.ADD: ParsersBundle([('StandardSyntax', additive_exp, not BLOCK_ERROR)], PredExprLevel.ADD),
  PredExprLevel.MUL: ParsersBundle([('StandardSyntax', multiplicative_exp, not BLOCK_ERROR)], PredExprLevel.MUL),
  PredExprLevel.UNARY: ParsersBundle([('StandardSyntax', unary_exp, not BLOCK_ERROR)], PredExprLevel.UNARY),
  PredExprLevel.VALUE: ParsersBundle([('StandardSyntax', value_exp, not BLOCK_ERROR)], PredExprLevel.VALUE),
  PredExprLevel.VAR: ParsersBundle([('StandardSyntax', var_exp, not BLOCK_ERROR)], PredExprLevel.VAR),
  PredExprLevel.TUPLE: ParsersBundle([('StandardSyntax', tuple_exp, not BLOCK_ERROR)], PredExprLevel.TUPLE),
  PredExprLevel.FUNC: ParsersBundle([('StandardSyntax', function_exp, not BLOCK_ERROR)], PredExprLevel.FUNC)
}

sql_clause_keywords = {
  SQLClause.SELECT: set(['select']),
  SQLClause.WHERE: set(['where'])
}