from typing import List, Union
import inspect

from .rule_operand import RuleOperand
from ...utils import *
from ...utils.exceptions import *
from ...ast import *
from ...utils.logger import Logger

logger = Logger.general_logger

class Rule(object):
  """Abstract class for rule object"""
  def __init__(self, operand: RuleOperand) -> None:
    self.operand = operand
    """Root operand"""
    
  def _identity(self):
    """
    Returns the identification for this rule instance
    Note that different RuleOperand trees may correspond to the same list by BFS, 
      so the list from self.operand.toClassList() should not be used as a unique signature for the rule.
    """
    return self.__class__ 
    
  """
    Sets need two methods to make an object hashable: __hash__ and __eq__. 
    Two instances must return the same hash value when they are considered equal. 
    An instance is considered already present in a set if both the hash is present in the set 
        and the instance is considered equal to one of the instances with that same hash in the set.
    Therefore, to store rules in Optimizer.ruleSet, Rule must implement both of __hash__ and __eq__ methods.
    * If a class doesn't implement __eq__, the default object.__eq__ is used instead, 
        which only returns true if 'obj1 is obj2' is also true. 
        In other words, by default two instances are only considered equal if they are the exact same instance.
    See https://stackoverflow.com/questions/38430277/python-class-hash-method-and-set for more details.
  """
  def __hash__(self):
    return hash(self._identity())

  def __eq__(self, other):
    return self._identity() == other._identity()

  def __str__(self):
    return ', '.join(list(map(str, self.operands)))

  def matches(self, ast_root: Expr) -> bool:
    """Checks whether this rule matches the current logical plan"""
    # The given plan root could be None.
    # But if it is not None, it must be an Expr instance.
    if ast_root is not None:
      ERROR_IF_NOT_INSTANCE_OF(ast_root, Expr, 
        "{}.matches(ast_root) only accepts an Expr as input ({} received)".format(getClassNameOfInstance(self), type(ast_root))
      )
    return self.operand.matches(ast_root)

  def transform(self, ast_root: Expr, inplace = False) -> Union[List[Expr], Expr]:
    """
    Calls 'transformImpl' to do the transfomation on a deep copy of the given expression tree, 
      (or calls 'transformInplInplace' to transform the given tree in-place),
      validates the results after transformation, 
      and finally returns the generated equivalent plan(s) if valid.

    Note that when not in-place, this method returns a list of expression trees, 
      since some rules may generate multiple equivalent plans;  
      while if in-place, this method returns a single expression tree, 
      and the rules calling this method with 'inplace = True' must generate only one equivalent plan. 
    """
    if not ast_root.isResolved():
      ERROR_IF_NONE(ast_root.schema, 
        "{}.transform{} requires input expression tree to have not-None schema on each node (Some nodes with NoneType schema received. The expression tree is probably not resolved.)"\
          .format(getClassNameOfInstance(self), inspect.signature(self.transform))
      )
    else:
      logger.warn("{}.transform{} received a expression tree with unknown resolve status."\
        .format(getClassNameOfInstance(self), inspect.signature(self.transform))
      )

    if inplace:
      equiv_plan = self.transformImplInplace(ast_root)
      # validate: must not None
      ERROR_IF_NONE(equiv_plan, 
        "method '{}.transformImplInplace' must return an Expr (None received).".format(getClassNameOfInstance(self)), 
        RuleImplementError)
      # validate: must return only one plan
      ERROR_IF_NOT_INSTANCE_OF(equiv_plan, Expr, 
        "method '{}.transformImplInplace' must return only one plan (a {} received)."\
          .format(getClassNameOfInstance(self), type(equiv_plan)), 
        RuleImplementError)
      return equiv_plan

    equiv_plans = self.transformImpl(ast_root)
    # validate: must not None
    ERROR_IF_NONE(equiv_plans, 
      "method '{}.transformImpl' must return a list (None received).".format(getClassNameOfInstance(self)), 
      RuleImplementError)
    # validate: must return a list of plans
    ERROR_IF_NOT_INSTANCE_OF(equiv_plans, list, 
      "method '{}.transformImpl' must return a list ({} received).".format(getClassNameOfInstance(self), type(equiv_plans)), 
      RuleImplementError)
    # validate: must return a non-empty list of plans
    ERROR_IF_FALSE(len(equiv_plans) > 0,  
      "method '{}.transformImpl' must return a non-empty list (empty list received).".format(getClassNameOfInstance(self)), 
      RuleImplementError)
    # validate: must deep copy the given plan and transform on the copy, 
    #   and the original input plan should not be modified. 
    ERROR_IF_FALSE(all(plan is not ast_root for plan in equiv_plans),  
      "method '{}.transformImpl' must deep copy the given plan and do the transformation on the copy.".format(getClassNameOfInstance(self)), 
      RuleImplementError)
    return equiv_plans

  def transformImpl(self, ast_root: Expr) -> List[Expr]:
    """
    The actual implementation of the transformation by the current rule.
    This method is left to the concrete rule classes to implement.

    Note that some rules may generate multiple equivalent plans, 
    so this method returns a list of expression trees.
    """
    raise NotImplementedError

  def transformImplInplace(self, ast_root: Expr) -> Expr:
    """
    The actual implementation of the inplace transformation by the current rule.
    This method is left to the concrete rule classes to implement.

    Note that this method transforms the expression tree in-place, 
    so the rules implementing this method must generate only one equivalent plan, instead of a list of plans.
    So for the rules which generate multiple equivalent plans, do not implement this method. 
    """
    raise NotImplementedError
