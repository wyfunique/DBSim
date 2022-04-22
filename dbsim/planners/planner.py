from typing import Set, Union, Dict, ClassVar
from .rules import *
from ..utils import *
from ..ast import *
from ..query import Query

class Planner(object):
  def __init__(self) -> None:
    self.locked: bool = False
    """Whether the optimizer can accept new rules"""
    self.rule_set: Set[Rule] = set()
    """Set to contain all the added rules"""
    self.root = None
    """
    The top node of the initial logical plan to be optimized.
    """
  
  def addRule(self, rule: Rule) -> bool:
    ERROR_IF_NOT_INSTANCE_OF(rule, Rule, "The rule to be added is not a Rule instance ({} received)".format(type(rule)))        
    if self.locked:
      # This optimizer does not accept new rules
      return False
    if rule in self.rule_set: 
      # Rule already exists.
      return False
    self.rule_set.add(rule)    
    return True
  
  def findBestPlan(self, plan: Expr) -> Expr:
    """
    Inputs a resolved expression tree,
      returns the optimized expression tree.

    Left for concrete planner subclass to implement.
    """
    raise NotImplementedError

  def optimizeQuery(self, query: Query) -> Query:
    """
    Returns the Query object with the best plan and corresponding schema 
      after applying a sequence of rules to the given query.
    """
    ERROR_IF_NOT_INSTANCE_OF(
      query, Query, 
      "'{}.optimizeQuery(query)' requires a Query object as input ({} received)"\
        .format(getClassNameOfInstance(self), type(query))
    )
    return Query(query.dataset, self.findBestPlan(query.operations))