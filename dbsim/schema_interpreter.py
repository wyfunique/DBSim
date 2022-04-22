"""
Module used to interpret the AST into a schema based on a given relation.
"""
import typing

from dbsim.utils import ERROR_IF_NOT_INSTANCE_OF, getClassNameOfClass
from dbsim.utils.exceptions import ExtensionInternalError

from . import Relation
from .schema import Schema,JoinSchema
from .operations import walk, visit_with, is_not

from .field import Field, FieldType
from .ast import (
  Expr, ProjectionOp, SelectionOp, GroupByOp, RenameOp, LoadOp,
  JoinOp, LeftJoinOp, SuperRelationalOp, UnionAllOp,
  Var, Function, 
  Const, UnaryOp, BinaryOp, AliasOp, SelectAllExpr,
  NumberConst, StringConst, BoolConst, NullConst, ParamGetterOp
)

ConstNodeToDataType: typing.Dict[Const, FieldType] = {
  NumberConst: FieldType.INTEGER,
  StringConst: FieldType.STRING,
  BoolConst: FieldType.BOOLEAN,
  NullConst: FieldType.NULL
}

def addDataTypes(new_nodetypes_and_datatypes: typing.Dict[typing.Type[Const], FieldType]):
  global ConstNodeToDataType
  for nodetype in new_nodetypes_and_datatypes:
    if not issubclass(nodetype, Const):
      raise ExtensionInternalError(
        "schema_interpreter.addDataTypes only accepts subclasses of 'Const' ({} received)"\
          .format(type(nodetype))
      )
  ConstNodeToDataType.update(new_nodetypes_and_datatypes)

def removeDataTypes(removed_nodetypes_and_datatypes: typing.Dict[typing.Type[Const], FieldType]):
  global ConstNodeToDataType
  for nodetype in removed_nodetypes_and_datatypes:
    if nodetype in ConstNodeToDataType:
      del ConstNodeToDataType[nodetype]

def addRelationOps(
  new_op_types_and_schema_funcs: 
    typing.Dict[typing.Type[SuperRelationalOp], typing.Callable[[SuperRelationalOp, 'DataSet'], Schema]]
) -> None:
  """
  Adds extended relational operators and the corresponding schema resolving functions plugged in from external.
  """
  global op_type_to_update_schema_func
  op_type_to_update_schema_func.update({
    op_type: update_op_schema(new_op_types_and_schema_funcs[op_type]) 
    for op_type in new_op_types_and_schema_funcs
  })
  
def removeRelationOps(
  removed_op_types_and_schema_funcs: 
    typing.Dict[typing.Type[SuperRelationalOp], typing.Callable[[SuperRelationalOp, 'DataSet'], Schema]]
) -> None:
  global op_type_to_update_schema_func
  for op_type in removed_op_types_and_schema_funcs:
    if op_type in op_type_to_update_schema_func:
      del op_type_to_update_schema_func[op_type]


def resolve_schema(dataset, operations, *additional_visitors) -> Expr:
  """
  Given an expresion tree return a new tree whose node's
  schema values are properly set.
  """

  visitors = additional_visitors + (
    # this visitor checks if the current node is NOT a Relation object, 
    # i.e., if it is a relational operator or function.
    # If true, calls the method 'resolve_schema_for_node' to set the correct schema for the current node;
    # else, do nothing since a Relation object has already had the correct schema at this step.
    (is_not(Relation), resolve_schema_for_node),
  )

  # traverse the AST and take actions by the visitors
  return walk(
    operations,
    visit_with(
      dataset,
      *visitors
    )
  )

def resolve_schema_for_node(dataset, loc, op):
  func_for_updating_op_schema = op_type_to_update_schema_func.get(
    type(op),
    update_op_schema(schema_from_relation)
  )
  return loc.replace(func_for_updating_op_schema(op, dataset))


def schema_from_relation(operation, dataset):
  """
  Returns the schema from the only child (stored as the 'relation' attr) of the operator. 
  This is the default method for any operator that doesn't modify the input schema.
  """
  return operation.relation.schema





# Function([relation_or_const]) -> Relation(Schema, (context-> [Tuples]))
def schema_from_function_op(operation, dataset):
  func = dataset.get_function(operation.name)

  # each arg should eithe ber a relation or a constant
  args = [
    a if hasattr(a,'schema') else a.const
    for a in operation.args
  ]

  if hasattr(func, 'resolve'):
    return func.resolve(func, dataset, *args)

  if isinstance(func.returns, Schema) :
    schema = func.returns
  elif callable(func.returns):   
    schema = func.returns(*args)


  # records is a partial that applies the args
  # along with given ctx to the func when called
  def records(ctx):
    return func(ctx, *args)

  return Relation(None, operation.name, schema, records)

def relational_function(dataset, op):
  """Invokes a function that operates on a whole relation"""
  return op.func



def schema_from_load(operation, dataset):

  return dataset.get_schema(operation.name)
  #return dataset.get_relation(operation.name).schema

def schema_from_projection_op(projection_op, dataset):
  """
  Given a projection_op, dataset, return the new
  schema.
  """

  schema = projection_op.relation.schema

  fields = [
    field
    for expr in projection_op.exprs
    for field in fields_from_expr(expr,dataset,schema)
  ]

  return Schema(fields=fields)

def schema_from_union_all(operation, dataset):
  l_schema = operation.left.schema
  r_schema = operation.right.schema
  if len(l_schema.fields) != len(r_schema.fields):
    raise RuntimeError(
      "Schemas from {} and {} must be of the same length.",
      operation.left.name,
      operation.right.name
    )
  for pos, fields in enumerate(zip(l_schema.fields, r_schema.fields)):
    left, right = fields
    if right.type not in (left.type, FieldType.NULL):
      raise RuntimeError(
        "Schemas at position {} have different types {} {}",
        pos,
        left.type,
        right.type
      )

  return l_schema

def schema_from_join_op(join_op, dataset):
  left  =  join_op.left.schema
  right =  join_op.right.schema
  return Schema(fields=left.fields + right.fields)
  #return JoinSchema(left,right)


def schema_from_alias_op(alias_op, dataset):
  schema = alias_op.relation.schema
  name = alias_op.name
  fields = [f.new(schema_name=name) for f in schema.fields]
  return schema.new(name=name, fields=fields)

def fields_from_expr(expr, dataset, schema):
  if isinstance(expr, SelectAllExpr):
    for field in fields_from_select_all(expr, dataset, schema):
      yield field
  else:
    yield field_from_expr(expr, dataset, schema)


def field_from_expr(expr, dataset, schema):
  expr_type = type(expr)
  if expr_type == Var:
    return field_from_var(expr, schema)
  elif issubclass(expr_type, Const):
    return field_from_const(expr)
  elif issubclass(expr_type, ParamGetterOp):
    return Field(name ='?column?', type = 'OBJECT')
  elif expr_type == Function:
    return field_from_function(expr, dataset, schema)
  elif expr_type == RenameOp:
    return field_from_rename_op(expr, dataset, schema)
  elif issubclass(expr_type, UnaryOp):
    field = field_from_expr(expr.expr, dataset, schema)
    return field.new(name="{0}({1})".format(expr_type.__name__, field.name))
  elif issubclass(expr_type, BinaryOp):
    lhs_field = field_from_expr(expr.lhs, dataset, schema)
    rhs_field = field_from_expr(expr.lhs, dataset, schema)
    if lhs_field.type != rhs_field.type:
      raise ValueError(
        "Can't coerce {} to {}".format(lhs_field.type, rhs_field.type)
      )
    else:
      return lhs_field.new(name="?column?".format(
        expr_type.__name__, 
        lhs_field.name,
        rhs_field.name
      ))



def fields_from_select_all(expr, dataset, schema):
  if expr.table is None:
    fields = schema.fields
  else:
    fields = [
      f
      for f in schema.fields
      if f.schema_name == expr.table
    ]
  return fields


def field_from_const(expr):
  global ConstNodeToDataType
  if type(expr) not in ConstNodeToDataType:
    raise ExtensionInternalError("Unrecognized operator: {}".format(type(expr)))
  return Field(
    name ='?column?',
    type = ConstNodeToDataType[type(expr)]
  )


def field_from_var(var_expr, schema):
  return schema[var_expr.path]

def field_from_function(function_expr, dataset, schema):
  name = function_expr.name
  function = dataset.get_function(function_expr.name)

  if function.returns:
    return function.returns
  #elif len(function_expr.args):
  #  # no return type specified guess the type based on the first
  #  # argument. Dataset.add_function should prevent functions
  #  # from being registered without args and return_types
  #  return field_from_expr(function_expr.args[0], dataset, schema)
  else:
    raise ValueError("Can not determine return type of Function {}".format(name))


def field_from_rename_op(expr, dataset, schema):
  field = field_from_expr(expr.expr, dataset, schema)
  return field.new(name=expr.name)

def replace_schema(op, schema):
  return op.new(schema=schema) # 'new' is a method that returns a deep copy of the existing object with the specified attrs overwritten, see immutable.py for details.

# :: (operation -> dataset -> schema) -> (operation, dataset) -> RelationalOp|(callable)
def update_op_schema(func):
  """
  Method to build a function for updating the current operator's output schema.

  This method applies a function on an operator with a dataset to generate a new schema, 
  and returns a function for replacing the operator's schema with the new one.
  In short, this method generates the function that computes and updates the current operator's output schema. 
  (Note: the returned function by this method ONLY updates the 'schema' attr of the input operator without changing any other attr.)

  Parameters:
    func: a function that returns a schema given an operator and a dataset
  Returns:
    A function. Its inputs are the current operator and dataset, 
    while its output is a deep copy of the input operator whose original schema is replaced with the returned schema from func.
    
    This returned function will be later used to initialize the operator's schema when building the Query object. 
    When query_parser builds the AST, each node/operator's output schema is undefined (i.e., set to None),
    so during the construction of a Query instance over the AST, 
    such functions are needed to replace the original undefined schemas (i.e., 'None') of the AST nodes with the correct schemas.  
  """
  def _(operation, dataset):
    schema = func(operation, dataset)
    return replace_schema(operation, schema)
  return _


op_type_to_update_schema_func = {
  # To initialize the regular operators, we just need to set their schema correctly; 
  # but for a relational function operator, more things need to be done, 
  # so Function type does not use 'update_op_schema' directly but uses some different method.
  LoadOp: update_op_schema(schema_from_load),
  ProjectionOp: update_op_schema(schema_from_projection_op),
  AliasOp: update_op_schema(schema_from_alias_op),
  UnionAllOp: update_op_schema(schema_from_union_all),
  JoinOp: update_op_schema(schema_from_join_op),
  LeftJoinOp: update_op_schema(schema_from_join_op),

  Function: schema_from_function_op,
}
"""
The operators listed in 'op_type_to_update_schema_func'
are those which may modify the schema from the children;
while those not listed here will definitely NOT change the schema.
For example, SelectionOp and all the operators in a predicate (like And, Or, GtOp, AddOp, etc.) 
are not listed here since a selection or a predicate will never change the schema from children. 
"""