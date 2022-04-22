from cmath import sqrt
from ...ast import *
from .registry_utils import *
from .extended_syntax import ExtendedSyntax
from ...utils.exceptions import SQLSyntaxError
from ... import query_parser_toolbox as toolbox 
from ...compilers import local as main_compiler
from ... import schema_interpreter  

from functools import partial
import typing

class SpatialSelectionOp(SelectionOp):
  __slots__ = ('relation','bool_op','schema', 'cost_factor', 'num_input_rows')
  def __init__(self, relation, bool_op, schema=None, cost_factor=DEFAULT_COST_FACTOR, num_input_rows=-1):
    super().__init__(relation, bool_op, schema, cost_factor, num_input_rows)

class InsideOp(BinaryOp):
  """
  lhs inside rhs,
  where lhs is a Point and rhs is a Circle.
  This operator checks if the Point is inside the Circle 
    and returns true/false. 
  """
  __slots__ = ('lhs', 'rhs', 'cost_factor')
  def __init__(self, lhs, rhs, cost_factor: float = DEFAULT_COST_FACTOR * 2):
    self.cost_factor = cost_factor
    self.lhs = lhs
    self.rhs = rhs
  def __str__(self):
    return "{} INSIDE {}".format(str(self.lhs), str(self.rhs))

class Point:
  __slots__ = ("x", "y")
  def __init__(self, x: float, y: float):
    self.x = x
    self.y = y
  def __str__(self) -> str:
    return "Point<{}, {}>".format(self.x, self.y)

class RelPoint(Const):
  __slots__ = ('const',)

  def __init__(self, point: Point):
    self.const = point
    self.cost_factor = TINY_COST_FACTOR * 2
  def __str__(self):
    return str(self.const)

class Circle:
  __slots__ = ("center", "radius")
  def __init__(self, center: Point, radius: float):
    self.center = center
    self.radius = radius
  def __str__(self) -> str:
    return "Circle{{ {}, r={} }}".format(str(self.center), self.radius)

class RelCircle(Const):
  __slots__ = ('const',)

  def __init__(self, circle: Circle):
    self.const = circle
    self.cost_factor = TINY_COST_FACTOR * 3
  def __str__(self):
    return str(self.const)

def distance(p1: Point, p2: Point) -> float:
  return sqrt((p1.x-p2.x)**2 + (p1.y-p2.y)**2).real

def isInside(p: Point, c: Circle):
  ERROR_IF_NOT_INSTANCE_OF(
    p, Point, "the first argument of 'isInside' must be a Point({} received)".format(type(p)), 
    SQLSyntaxError
  )
  ERROR_IF_NOT_INSTANCE_OF(
    c, Circle, "the second argument of 'isInside' must be a Circle({} received)".format(type(c)), 
    SQLSyntaxError
  )
  return distance(p, c.center) < c.radius

def get_inside_op_executor():
  return partial(main_compiler.binary_op, isInside)

def parse_point(tokens: TokenList) -> Point:
  token = tokens[0]
  if token == '#':
    # parse '#x, y#' as a Point literal
    tokens.pop(0)
    if len(tokens) == 0:
      raise SQLSyntaxError("missing closing '#'")
    token = tokens.pop(0)
    cur_element = ""
    cur_coordinate = 0
    x, y = None, None
    while token != '#':
      if not token.isdigit() and token not in (',', '.'):
        raise SQLSyntaxError("invalid symbol found in vector.")
      if token.isdigit() or token == '.':
        cur_element += token
      elif token == ',':
        try:
          cur_element = float(cur_element)
        except:
          raise SQLSyntaxError("invalid vector element: '{}'".format(cur_element))
        if cur_coordinate == 0:
          x = cur_element
        elif cur_coordinate == 1:
          y = cur_element
        else:
          raise SQLSyntaxError("Higher dimensional points than 2D are not supported.")
        cur_element = ""
      if len(tokens) == 0:
        raise SQLSyntaxError("missing closing '#'")
      token = tokens.pop(0)
    if len(cur_element) > 0:
      if x is None:
        raise SQLSyntaxError("1D points are not supported.")
      elif y is None:
        y = float(cur_element)
      else:
        raise SQLSyntaxError("Higher dimensional points than 2D are not supported.")
    return Point(x, y)
  else:
    raise ParsingFailure

def parse_circle(tokens: TokenList) -> Circle:
  token = tokens[0]
  if token == '{':
    # parse '{#x, y#, r}' as a Circle literal
    tokens.pop(0)
    if len(tokens) == 0:
      raise SQLSyntaxError("missing closing '}'")
    token = tokens[0]
    if token != '#':
      raise SQLSyntaxError("missing center in a circle")
    center = parse_point(tokens)
    ERROR_IF_NONE(center, "Unknown error in parse_point(tokens)", SQLSyntaxError)
    token = tokens.pop(0)
    radius = ""
    if token != ',':
      raise SQLSyntaxError("unexpected token '{}' after center claim in circle.".format(token))
    token = tokens.pop(0)
    while token != '}':
      if not token.isdigit() and token != '.':
        raise SQLSyntaxError("invalid symbol '{}' found in circle.".format(token))
      radius += token
      if len(tokens) == 0:
        raise SQLSyntaxError("missing closing '}'")
      token = tokens.pop(0)
    try:
      radius = float(radius)
    except:
      raise SQLSyntaxError("invalid radius: '{}'".format(radius))
    return Circle(center, radius)
  else:
    raise ParsingFailure

def parse_point_and_circle_exp(tokens: TokenList) -> Expr:
  token = tokens[0]
  if token == '#':
    # parse it as a Point literal
    return RelPoint(parse_point(tokens))
  elif token == '{':
    # parse it as a Circle literal
    return RelCircle(parse_circle(tokens))
  else:
    raise ParsingFailure

def parse_inside_exp(tokens: TokenList) -> Expr:
  lhs = toolbox.predicate_parsers[PredExprLevel.ADD](tokens)
  if len(tokens):
    ERROR_IF_NOT_EQ(tokens[0], 'inside', "keyword 'inside' not found", ParsingFailure)
    tokens.pop(0)
    rhs = toolbox.predicate_parsers[PredExprLevel.ADD](tokens)
    lhs = InsideOp(lhs, rhs)
    return lhs
  else:
    raise ParsingFailure

class SpatialSyntax(ExtendedSyntax):
  """
  Example #1:
    spatialselect pid 
    from points 
    where point inside {#0,0#, 3}
  """
  spatialselect_keyword = 'spatialselect'

  _extended_symbols_: str = '#{}'
  _extended_clause_keywords_ = {SQLClause.SELECT: spatialselect_keyword}
  _extended_data_types_: typing.Dict[typing.Type[Const], str] = {RelPoint: 'POINT', RelCircle: 'CIRCLE'}
  _extended_predicate_parsers_ = ({
    PredExprLevel.COMP: parse_inside_exp,
    PredExprLevel.VALUE: parse_point_and_circle_exp
  }, False)
  _extended_predicate_op_executors_: \
      typing.Dict[typing.Type[SimpleOp], typing.Callable[[typing.Any], typing.Callable]] = \
        {InsideOp: get_inside_op_executor()}
  _extended_relation_op_schema_: \
      typing.Dict[typing.Type[SuperRelationalOp], typing.Callable[[SuperRelationalOp, 'DataSet'], 'Schema']] = \
        {SpatialSelectionOp: schema_interpreter.schema_from_relation}
  _extended_relation_op_executors_: \
      typing.Dict[typing.Type[SuperRelationalOp], typing.Callable[[typing.Any], typing.Callable]] = \
        {SpatialSelectionOp: main_compiler.selection_op}

  
  def __init__(self):
    self.spatialselect_detected: bool = False
    """
    A flag for whether the query is a standard select query or a spatialselect query.
    This is used by parse_spatialselect_where to determine whether to throw an error when keyword 'inside' is not met.
    
    Specifically, when 'spatialselect' is detected, there must be at least one 'inside' 
      existing in either the SELECT cluase or the WHERE clause of the current query, 
      otherwise a syntax error should be thrown. 
    """
    self.inside_keyword_detected_in_select_clause: bool = False
    """
    A flag for whether 'inside' keyword is found in the SELECT clause of the current query.
    If it is found, a SpatialSelectionOp instead of SelectionOp will be generated, 
      whether 'spatialselect' is used or not.
    If it is not found in SELECT clause, it will be checked in WHERE clause later, 
      and if found there, a SpatialSelectionOp will be generated.
    Only when there is no 'inside' and no 'spatialselect' keywords detected in both of SELECT and WHERE clauses, 
      the query will be parsed to a standard selection (SelectionOp).  
    
    Note that the FROM clause will not be checked for 'inside' 
      since 'inside' is a predicate operator (i.e., should only present in predicate expressions) 
      instead of a relational operator like join. 
    So if 'inside' is found in FROM clause, it must be in a nested query, 
      which has no impact on the parsing of the current query level. 
    """
    self.I_am_triggered: bool = False
    """
    Flag to tell 'trigger_spatialselect_where' if 'trigger_spatialselect' returns True.
    If yes, 'trigger_spatialselect_where' will always return True.
    Otherwise 'trigger_spatialselect_where' will do its own checking and return True or False.
    """

  def trigger_spatialselect(self, tokens: TokenList) -> bool:
    try:
      index_of_from_clause = tokens.index('from')
      # 'from' exists in tokens, there may be nested queries.
      # In such case only needs to check the tokens before 'from', 
      #   and the nested queries will be checked later 
      #   during the recursive parsing for the inner select statements.
      self.I_am_triggered = \
        tokens[0] == self.spatialselect_keyword \
        or (tokens[0] == 'select' and 'inside' in tokens[:index_of_from_clause])
      return self.I_am_triggered
    except ValueError:
      # no 'from' in tokens, i.e., there are no nested queries
      self.I_am_triggered = \
        tokens[0] == self.spatialselect_keyword \
        or (tokens[0] == 'select' and 'inside' in tokens)
      return self.I_am_triggered
    except Exception as e:
      raise e

  def parse_spatialselect(self, tokens: TokenList) -> List[Expr]:
    # resets the flags to avoid the influence from previous queries 
    self.spatialselect_detected = False
    self.inside_keyword_detected_in_select_clause = False

    if tokens[0] not in ('select', self.spatialselect_keyword):
      raise SQLSyntaxError
    keyword = tokens.pop(0)
    if keyword == self.spatialselect_keyword:
      # 'spatialselect' detected
      self.spatialselect_detected = True
    for token in tokens:
      if token in ('from', 'where'):
        # the current SELECT clause ends at 'from' or 'where' or the tail of tokens
        break
      if token == 'inside':
        # 'inside' detected in the current SELECT clause
        self.inside_keyword_detected_in_select_clause = True
    
    # Whether the extended keywords 'spatialselect' and 'inside' are found or not, 
    #   simply does the same thing as standard select, 
    #   i.e., using select_core_exp to parse the columns to be selected  
    select_cols = toolbox.select_core_exp(tokens) 
    return select_cols 
  
  def trigger_spatialselect_where(self, tokens: TokenList) -> bool:
    return self.I_am_triggered or (tokens[:1] == ['where'] and 'inside' in tokens)

  def parse_spatialselect_where(self, tokens: TokenList, relation: Expr) -> Expr:
    if tokens[:1] == ['where']:
      tokens.pop(0)
      # validate the syntax requirements on the occurrance of 'spatialselect' and 'inside'
      inside_keyword_detected_in_where_clause = False
      for token in tokens:
        if token in ('select', 'from', 'where', self.spatialselect_keyword):
          # If any one among the 'select', 'from', 'where' and 'spatialselect' keywords is met, 
          #   it means the current WHERE clause is part of a nested query, 
          #   and the current WHERE clause reaches at most the token right before the met keyword,
          #   i.e., the met keyword can be used as a rough boundary of the current WHERE clause
          #   for the purpose of checking occurrance of 'inside'.
          break
        if token == 'inside':
          # 'inside' detected in the current WHERE clause
          inside_keyword_detected_in_where_clause = True
      if self.spatialselect_detected:
        ERROR_IF_FALSE(
          self.inside_keyword_detected_in_select_clause or inside_keyword_detected_in_where_clause, 
          "spatialselect requires at least one 'inside' expression, zero found.", 
          ExtendedSyntaxError
        )
      if inside_keyword_detected_in_where_clause or self.inside_keyword_detected_in_select_clause or self.spatialselect_detected: 
        # if 'inside' keyword is found in WHERE clause or in SELECT clause, 
        #   or the keyword 'spatialselect' instead of 'select' is used in SELECT clause, 
        #   calls extended_where_core_expr here to generate a SpatialSelectionOp
        relation: SpatialSelectionOp = self.extended_where_core_expr(tokens, relation)
      else:
        # otherwise, calls the standard where_core_expr to generate a standard SelectionOp
        relation: SelectionOp = toolbox.where_core_expr(tokens, relation)
    else:
      # The current query has no WHERE clause,  
      #   but we still need to validate the syntax requirements 
      #   on the occurrance of 'spatialselect' and 'inside'.
      if self.spatialselect_detected:
        ERROR_IF_FALSE(
          self.inside_keyword_detected_in_select_clause,
          "spatialselect requires at least one 'inside' expression, zero found.", 
          ExtendedSyntaxError
        )

    # resets the flags to avoid affecting the parsing of future queries 
    #self.spatialselect_detected = False
    #self.inside_keyword_detected_in_select_clause = False
    return relation

  def extended_where_core_expr(self, tokens: TokenList, relation: Expr) -> SpatialSelectionOp:
    return SpatialSelectionOp(relation, toolbox.or_exp(tokens))

 
