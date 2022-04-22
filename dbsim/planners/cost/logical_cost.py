from ...ast import *
from ...compilers import local as main_compiler
from ...dataset import DataSet
from ...query import Query

class LogicalCost(object):
  """
  Logical cost.

  The 'cost' in general query optimization normally refers to the cost of physical plan, 
    which includes the cost from CPU, IO, network, etc. 
  
  Unlike that kind of cost, 'logical cost' is computed purely based on the logical plan.
  For example, the total number of rows in all the intermediate results. 
  Logical cost needs the general stat info, like number of rows in the input relations, 
    but has nothing to do with the physical implementation.   
  """
  __slots__ = ("value", )

  def __init__(self, value: float = -1):
    self.value = value

  def toNumeric(self):
    return self.value

  @classmethod
  def refineCostFactors(cls, node: Expr, is_predicate: bool) -> None:
    """
    Recomputes the cost_factor of each resolved plan node
      based on the initial cost_factor values.

    Roughly, the refined cost_factor of each node 
      is summation of the initial cost_factors of itself and all operators in its predicate. 
    """
    if is_predicate:
      refined_cost_factor = node.getCostFactor()
      for child in getChildren(node):
        cls.refineCostFactors(child, True)
        refined_cost_factor += child.getCostFactor()
      node.setCostFactor(refined_cost_factor)
      return

    # otherwise, this is a relational op, should be treated differently from predicate op
    if isinstance(node, (LoadOp, Relation)):
      # reaches leaf node, does nothing and just returns 
      return
    
    # initial cost_factor of the node itself
    refined_cost_factor = node.getCostFactor()
    ERROR_IF_NONE(refined_cost_factor, "{} has no 'cost_factor' attribute.".format(getClassNameOfInstance(node)))
    if hasattr(node, 'exprs'):
      predicates: List[Expr] = node.exprs
      for predicate in predicates:
        ERROR_IF_NOT_INSTANCE_OF(predicate, SimpleOp, 
          "requires predicate to be a SimpleOp instance ({} received)".format(type(predicate)))
        cls.refineCostFactors(predicate, True)
        refined_cost_factor += predicate.getCostFactor()
    elif hasattr(node, 'bool_op'):
      predicate: Expr = node.bool_op
      ERROR_IF_NOT_INSTANCE_OF(predicate, SimpleOp, 
          "requires predicate to be a SimpleOp instance ({} received)".format(type(predicate)))
      cls.refineCostFactors(predicate, True)
      # added with the refined cost_factor of the predicate in the current plan node,
      #   i.e., the summation of initial cost_factors of all operators 
      #   in the predicate expression tree.
      refined_cost_factor += predicate.getCostFactor()
    # sets the cost_factor of the current plan node to the refined value
    node.setCostFactor(refined_cost_factor)
    # recursively computes and sets the refined cost_factors for the descendants 
    for child in getChildren(node):
      cls.refineCostFactors(child, False)
    

  @classmethod
  def getCost(cls, plan: Expr, dataset: DataSet) -> 'LogicalCost':
    """
    Computes and returns the logical cost of the given plan.
    Formula:

        (1) logical cost = sum(logical costs of all relational operators)
        
        (2) logical cost of single relational operator = its_number_of_processed_rows * its_refined_cost_factor
    
    So this method MUST be called AFTER LogicalCost.refineCostFactors.

    Note that 'number_of_processed_rows' is not always the total number of rows from the children nodes.
    For nodes with only one child, like projection and selection, that is the number of rows output by the only child;
      while for nodes with two children, like join and union, the cases are more complex:
        (1) for union, its number_of_processed_rows is the summation of the #rows output by two children;
        (2) for join, instead of summation, the number_of_processed_rows is the multiplication of the #rows output by two children
      etc.
    """
    ERROR_IF_FALSE(
      plan.isResolved(), 
      "requires a resolved plan (unresolved plan received)"
    )
    ctx = {
      'dataset': dataset
    }
    dataset.execute(Query(dataset, plan, resolve_op_schema=False), ctx=ctx)
    stat_info_field = main_compiler.stat_field_in_ctx
    cost_info_field = main_compiler.num_input_rows_and_cost_factor_field
    cost = 0.0
    for num_processed_rows, cost_factor in ctx[stat_info_field][cost_info_field]:
      cost += num_processed_rows * cost_factor
    return cls(cost)
  