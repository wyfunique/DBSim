from typing import List, Union
from ...utils import *
from ...utils import exceptions
from ...ast import *

import inspect

class RuleOperand(object):
  """The operand tree to represent a rule"""
  def __init__(self, ast_root_class, child_operand_list):
    ERROR_IF_FALSE(
      ast_root_class is None or inspect.isclass(ast_root_class), 
      "Parameter 'ast_node_class' is not a class ({} received).".format(type(ast_root_class)), 
      PlannerInternalError
    )
    ERROR_IF_NOT_INSTANCE_OF(
      child_operand_list, list, 
      "Parameter 'child_operand_list' is not a list ({} received).".format(type(child_operand_list)), 
      PlannerInternalError
    )
    ERROR_IF_COMPARISON_FALSE(
      len(child_operand_list), 2, Comparison.LEQ,  
      "'child_operand_list' is required to include at most 2 elements ({} received).".format(len(child_operand_list)), 
      PlannerInternalError
    )
        
    self.ast_root_class = ast_root_class
    self.children = child_operand_list
  
  def __str__(self):
    return "<RuleOperand: {}>".format(getClassNameOfClass(self.ast_root_class))

  def toClassList(self):
    """
    Convert this RuleOperand tree to a list of the RuleOperand.ast_root_class by BFS traversal
    Note that different RuleOperand trees may correspond to the same list by BFS, 
      so this list should not be used as a unique signature for the rule.  
    """
    res = []
    q = [self]
    while len(q) > 0:
      operand = q.pop(0)
      res.append(operand.ast_root_class)
      for child in operand.children:
        q.append(child)
    return res

  def toOperandList(self):
    """
    Convert this RuleOperand tree to a flatten list of RuleOperands by preorder traversal
    Note that different RuleOperand trees may correspond to the same preorder list, 
      so this list should not be used as a unique signature for the rule.  
    """
    res = [self]
    if len(self.children) == 0:
      return res
    for child in self.children:
      res += child.toOperandList()
    return res

  def matches(self, ast_root: Union[RelationalOp, BinRelationalOp]):
    """Checks whether the current rule/operand tree matches the given logical plan"""
    if isinstance(self, AnyMatchOperand):
      # if this operand is AnyMatchOperand, it will always match the current AST subtree.
      # So no need to inspect the children of the ast_root and directly returns True.
      return True
    if isinstance(self, NoneOperand):
      return True if ast_root is None else False
    if ast_root is None:
      return True if isinstance(self, NoneOperand) else False
    if getClassNameOfInstance(ast_root) != getClassNameOfClass(self.ast_root_class):
      return False
    ast_children = getChildren(ast_root)
    if len(ast_children) > len(self.children):
      # If the number of ast children is larger than operand children, 
      #   the ast is impossible to match the rule.
      # But if the former is less than the latter, 
      #   it could be a match as long as the extra operand children are AnyMatchOperand.   
      return False
    i = 0
    while i < len(ast_children):
      if not self.children[i].matches(ast_children[i]):
        return False
      i += 1
    while i < len(self.children):
      # if len(self.children) > ast_children, continues checking the rest of self.children
      if not isinstance(self.children[i], AnyMatchOperand)\
        and not isinstance(self.children[i], NoneOperand):
          return False
      i += 1
    return True

class NoneOperand(RuleOperand):
  """Operand that only matches empty ast node"""
  def __init__(self) -> None:
    super().__init__(None, [])

class AnyMatchOperand(RuleOperand):
  """Operand that matches any ast node"""
  def __init__(self) -> None:
    super().__init__(None, [])