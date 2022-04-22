from typing import Union, List

from dbsim.utils import ERROR_IF_NOT_EQ
from .rule_operand import RuleOperand, NoneOperand, AnyMatchOperand
from .rule import Rule
from ...ast import *
from ...utils import exceptions

class FilterMergeRule(Rule):
  def __init__(self) -> None:
    super().__init__(
      RuleOperand(SelectionOp, [
        RuleOperand(SelectionOp, [AnyMatchOperand()])
      ])
    )
    
  def _transformImpl(self, ast_root: Expr, inplace: bool = False) -> Union[Expr, List[Expr]]:
    """
    Merges the sequential SelectionOps and returns the merged plan.
    If inplace is False, does the transformation on a copy of the original input plan 
        and returns the transformed plan, without modifying the original plan.  
    """
    assert self.matches(ast_root)
    if inplace:
      copy_ast = ast_root
    else:
      copy_ast = deepCopyAST(ast_root)
    upper_selection = copy_ast
    lower_selection = copy_ast.relation
    ERROR_IF_NOT_EQ(upper_selection.schema, lower_selection.schema, 
      "The sequential SelectionOps have different schemas.", 
      exceptions.PlannerInternalError
    )
    lower_selection.bool_op = And(lower_selection.bool_op, upper_selection.bool_op)
    upper_selection.relation = None
    return lower_selection if inplace else [lower_selection]

  def transformImpl(self, ast_root: Expr) -> List[Expr]:
    """Merges the sequential SelectionOps and returns a copy of the merged plan"""
    return self._transformImpl(ast_root, inplace=False)

  def transformImplInplace(self, ast_root: Expr) -> Expr:
    """Merges the sequential SelectionOps inplace and returns the plan root after transformation"""
    return self._transformImpl(ast_root, inplace=True)
        
        
        