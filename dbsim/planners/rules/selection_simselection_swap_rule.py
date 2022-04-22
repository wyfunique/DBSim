from typing import Union, List

from dbsim.utils import ERROR_IF_NOT_EQ
from .rule_operand import RuleOperand, NoneOperand, AnyMatchOperand
from .rule import Rule
from ...ast import *
from ...utils import exceptions

from ...extensions.extended_syntax import SimSelectionSyntax, SimSelectionOp

class Selection_SimSelection_Swap_Rule(Rule):
  def __init__(self) -> None:
    super().__init__(
      RuleOperand(SelectionOp, [
        RuleOperand(SimSelectionOp, [AnyMatchOperand()])
      ])
    )
    
  def _transformImpl(self, ast_root: Expr, inplace: bool = False) -> Union[Expr, List[Expr]]:
    """
    Swaps the order of sequential SelectionOp and SimSelectionOp and returns the swapped plan.
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
      "The sequential SelectionOp and SimSelectionOp have different schemas.", 
      exceptions.PlannerInternalError
    )
    upper_selection.relation = lower_selection.relation
    lower_selection.relation = upper_selection
    return lower_selection if inplace else [lower_selection]

  def transformImpl(self, ast_root: Expr) -> List[Expr]:
    return self._transformImpl(ast_root, inplace=False)

  def transformImplInplace(self, ast_root: Expr) -> Expr:
    return self._transformImpl(ast_root, inplace=True)
        
        
        