import typing
from typing import Union, List
from copy import deepcopy

from dbsim.schema import Schema

from .rule_operand import RuleOperand, NoneOperand, AnyMatchOperand
from .rule import Rule
from ...ast import *
from ...utils import * 
from ...utils.exceptions import *
from ...utils.predicate_utils import PredicateUtils

class FilterPushDownRule(Rule):
  """Implementation of the predicate-push-down rule"""
  def __init__(self) -> None:
    super().__init__(
      RuleOperand(SelectionOp, [
        RuleOperand(JoinOp, [AnyMatchOperand(), AnyMatchOperand()])
      ])
    )
    
  def _transformImpl(self, ast_root: Expr, inplace: bool = False) -> Union[Expr, List[Expr]]:
    """
    Decorrelates the selection predicate by 'AND' and pushes down those sub-predicates 
      which are related to only one child of the join. 
    If inplace is False, does the transformation on a copy of the original input plan 
      and returns the transformed plan, without modifying the original plan.  
    """
    assert self.matches(ast_root)
    ERROR_IF_FALSE(
      ast_root.isResolved(), 
      "unresolved plan received", 
      PlannerInternalError
    )
    if inplace:
      copy_ast = ast_root
    else:
      copy_ast = deepCopyAST(ast_root)
    selection = copy_ast
    join = copy_ast.relation
    ERROR_IF_NOT_EQ(selection.schema, join.schema, 
      "Invalid resolved plan received (the SelectionOp has different schema from its child JoinOp).", 
      PlannerInternalError
    )
    decorrelated_predicates: Dict[typing.Tuple[int], Predicate] =\
      PredicateUtils.decorrelateAnd(Predicate.fromRelationOp(selection), [join.left, join.right])
    top_selection_remains = False
    for related_node_indices in decorrelated_predicates:
      predicate = decorrelated_predicates[related_node_indices]
      if len(related_node_indices) == 0:
        # the predicate is not related to either join.left or join.right, 
        #   i.e., it is a constant expression like '1 = 1', 
        #   so we simply push it down to both join.left and right.
        join.left = SelectionOp(join.left, predicate.toExpr(), schema=join.left.schema.copy())
        join.right = SelectionOp(join.right, predicate.toExpr(), schema=join.right.schema.copy())
      elif len(related_node_indices) == 1:
        if related_node_indices[0] == 0:
          # push down to left child of join
          join.left = SelectionOp(join.left, predicate.toExpr(), schema=join.left.schema.copy())
        elif related_node_indices[0] == 1:
          # push down to right child of join
          join.right = SelectionOp(join.right, predicate.toExpr(), schema=join.right.schema.copy())
      else:
        # The predicate is related to both join.left and right, like 'column_in_left = column_in_right',
        #   then this predicate should not be pushed down, and the top selection will remain there.
        # If the top selection remains, remember to update its predicate. 
        
        # Update the predicate of top selection by replacing it with a new SelectionOp with the updated predicate. 
        selection = SelectionOp(join, predicate.toExpr(), schema=selection.schema.copy())
        top_selection_remains = True
    
    # if all decorrelated predicates are pushed down, the top selection will be removed and the join will become new root.
    if top_selection_remains:
      new_root = selection
    else:
      selection.relation = None
      new_root = join

    return new_root if inplace else [new_root]

  def transformImpl(self, ast_root: Expr) -> List[Expr]:
    return self._transformImpl(ast_root, inplace=False)

  def transformImplInplace(self, ast_root: Expr) -> Expr:
    return self._transformImpl(ast_root, inplace=True)
        
        
        