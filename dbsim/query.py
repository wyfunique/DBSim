from .ast import LoadOp, Expr, deepCopyAST
from .operations import isa
from .schema_interpreter import resolve_schema
from .utils import *

class Query(object):
  __slots__ = {
    'dataset': '-> DataSet',
    'operations': 'ast.Expr',
    'schema': 'Schema'
  }


  def __init__(self, dataset, operations: Expr, resolve_op_schema: bool = True, optimizer = None):
    self.dataset = dataset

    if resolve_op_schema:
      # Forces to resolve the schemas of the given expression tree
      #   whether it has been resolved or not.
      self.operations = resolve_schema(
        dataset, 
        operations, 
        (isa(LoadOp), view_replacer)
      )
    else:
      if not operations.isResolved():
        # The given expression tree is unresolved.
        raise PlannerInternalError("The input expression tree ('operations') must be resolved when 'resolve_op_schema' is False.")
      self.operations = deepCopyAST(operations)

    self.schema = self.operations.schema

    if optimizer is not None:
      # replace the initial query plan with the optimized plan
      self.operations = optimizer.findBestPlan(self.operations)

  def __iter__(self):
    return iter(self.execute())
  
  def getPlan(self) -> Expr:
    return self.operations

  def dump(self):
    self.dataset.dump(self.schema, self.execute())

  def create_view(self, name):
    self.dataset.create_view(name, self.operations)
  
  # This method returns a generator of the resulting rows
  def execute(self, *params):
    return self.dataset.execute(self, *params)

  # This method returns a list of the resulting rows, 
  # which are easier to be used than the generator.
  def get_pretty_results(self, *params):
    return list(self.execute(*params))

  def get_pretty_results_with_headers(self, *params):
    return self.get_pretty_results(*params), self.schema.get_fields_names()

def view_replacer(dataset, loc, op):
  view = dataset.get_view(op.name)

  if view:
    new_loc = loc.replace(view).leftmost_descendant()
    # keep going until the leftmost_descendant isn't a view
    return view_replacer(
      dataset,
      new_loc,
      new_loc.node()
    )
  else:
    return load_relation(dataset, loc, op)
 

def load_relation(dataset, loc, op):
  adapter = dataset.adapter_for(op.name)
  return adapter.evaluate(loc)


