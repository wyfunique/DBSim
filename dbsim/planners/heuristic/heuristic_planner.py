from enum import Enum
import logging
from typing import List
import inspect

from dbsim.query import Query

from ..planner import Planner
from ...ast import *
from ..rules import Rule
from ...utils import *
from ...utils.exceptions import *
from ...utils.logger import Logger
from ..cost.logical_cost import LogicalCost

logger = Logger.general_logger
debugger = Logger.finer_logger

class PlanMatchOrder(Enum):
  DepthFirstOrder = 1
  TopologicalOrder = 2
  #ReversedTopoOrder = 3

class HeuristicPlanner(Planner):
  """The rule-based heuristic optimizer"""
  __slots__ = ("match_order", "rule_seq", "max_num_applications")

  def __init__(self, match_order: PlanMatchOrder = PlanMatchOrder.DepthFirstOrder, max_limit: int = 100) -> None:
    super().__init__()
    self.match_order: PlanMatchOrder = match_order
    self.rule_seq: List[Rule] = list()
    ERROR_IF_FALSE(max_limit > 0, 
      "the limit for maximum number of rule applications must be positive ({} received).".format(max_limit))
    self.max_num_applications: int = max_limit

  def addRule(self, rule: Rule) -> bool:
    state_code = super().addRule(rule)
    if state_code:
      self.rule_seq.append(rule)
    return state_code

  def getRules(self) -> List[Rule]:
    return self.rule_seq
  
  def getRulesNames(self) -> List[str]:
    return [getClassNameOfInstance(rule) for rule in self.getRules()]

  def clearRules(self) -> None:
    self.rule_set = set()
    self.rule_seq = list()

  def setRoot(self, new_root: Expr) -> bool:
    if new_root is self.root:
      return False
    self.root = deepCopyAST(new_root)
    return True

  def getNodeAndParentIter(self, starting_node: Expr) -> Expr:
    """
    Returns a tuple iterator of each node, its parent, and the child index of the node to its parent 
      (this index is 0 for left child and 1 for right child) 
      over the expression subtree rooted at 'starting_node'.
    This iterator moves to the next by 'iterator = next(iterator)'.

    Note that the logical plan in DBSim is always a tree instead of DAG, 
      so topological order here is equivalent to breadth-first-order, 
      thus the traversal is implemented by BFS when self.match_order == PlanMatchOrder.TopologicalOrder.
    """
    if self.match_order == PlanMatchOrder.DepthFirstOrder:
      iter = traverseWithParent(starting_node, None, 0, 'dfs')
    elif self.match_order == PlanMatchOrder.TopologicalOrder:
      iter = traverseWithParent(starting_node, None, 0, 'bfs')
    return iter

  def applyRule(self, node: Expr, rule: Rule) -> Expr:
    """
    Checks if the rule matches the expression tree (or subtree) rooted at node.
    If not match, does nothing and returns None. 
    If matches, applies the rule to transforming the tree or subtree.
    After the transformation, 
      if the newly generated tree or subtree is the same as the input tree, 
      to avoid infinite loop in 'findBestPlan', returns None in such case;
      if the generated tree is different from the input, then returns the new tree.  
    """
    ERROR_IF_NOT_INSTANCE_OF(
      node, Expr, 
      "'{}.applyRule{}' requires an Expr object as the first argument ({} received)"\
        .format(getClassNameOfInstance(self), inspect.signature(self.applyRule), type(node))
    )
    ERROR_IF_FALSE(node.isResolved(), 
      "'{}.applyRule{}' requires a resolved expression tree as input (unresolved tree received)"\
        .format(getClassNameOfInstance(self), inspect.signature(self.applyRule)), 
        PlannerInternalError
    )
    ERROR_IF_NONE(self.root, "planner hasn't setup any plan root.", PlannerInternalError)

    if not rule.matches(node):
      return None
    equiv_plans: List[Expr] = rule.transform(node)
    best_plan = None
    if len(equiv_plans) == 1:
      # there is only one equivalent plan generated 
      best_plan = equiv_plans[0]
    else:
      # multiple equivalent plans generated, selects the best plan with lowest logical cost
      lowest_cost = float('Inf')
      for plan in equiv_plans:
        LogicalCost.refineCostFactors(plan, False)
        cur_cost = LogicalCost.getCost(plan).toNumeric()
        if cur_cost < lowest_cost:
          best_plan = plan
          lowest_cost = cur_cost
    
    ERROR_IF_NONE(best_plan, "Something wrong with the equivalent plans", PlannerInternalError)

    if equalResolvedExprs(node, best_plan):
      # the current rule did not change the original plan
      logger.debug("Applying rule '{}' did not change the original plan.".format(getClassNameOfInstance(rule))) 
      return None

    return best_plan

  def findBestPlan(self, plan: Expr) -> Expr:
    """
    Returns the best plan (an optimized expression tree) 
      after applying a sequence of rules to the input plan.

    This process ends when the number of rule applications reaches a maximum limit 
      or no more rules can be applied. 
    """
    ERROR_IF_NOT_INSTANCE_OF(
      plan, Expr, 
      "'{}.findBestPlan(plan)' requires an Expr object as input ({} received)"\
        .format(getClassNameOfInstance(self), type(plan))
    )
    ERROR_IF_FALSE(plan.isResolved(), 
      "'{}.findBestPlan{}' requires a resolved expression tree as input (unresolved tree received)"\
        .format(getClassNameOfInstance(self), inspect.signature(self.findBestPlan)), 
        PlannerInternalError
    )
    if self.root is not None:
      # clear the trace of the previous input plan
      self.setRoot(None)
    self.setRoot(plan)
    ERROR_IF_NONE(self.root, "planner hasn't setup any plan root.", PlannerInternalError)
    if len(self.rule_seq) == 0:
      logger.warn("planner was called without adding any rule.")
      return deepCopyAST(self.root)

    num_rules_applied = 0
    # iterates over all nodes of the current plan
    iter = self.getNodeAndParentIter(self.root)
    while num_rules_applied <= self.max_num_applications:
      # to avoid 'next(iter)' throwing StopIteration exception when reaching the end, 
      #   use 'next(iter, default_value)' instead, 
      #   then it will return the default_value when being at the end.
      node, parent, child_idx = next(iter, (None, None, -1))
      debugger.debug("on node {}".format(str(node)))
      if node is not None:  
        # tries to match and apply all the given rules to the current node 
        for rule in self.rule_seq:
          new_node = self.applyRule(node, rule)
          debugger.debug("applyRule on node {} returned {}".format(str(node), str(new_node)))    
          if new_node is not None:
            # the current rule was matched and applied
            # so the current plan will be replaced with the newly generated one.
            if node is self.root:
              # the whole plan was transformed, 
              # so we need to directly update self.root
              debugger.debug("the whole plan rooted at node {} transformed to new plan rooted at {}"\
                .format(str(node), str(new_node)))    
              self.setRoot(new_node)
            else:
              # only a sub-plan was transformed, 
              # thus we only need to replace the subtree
              debugger.debug("a sub-plan rooted at node {} (parent: {}) transformed to new sub-plan rooted at {}"\
                .format(str(node), str(parent), str(new_node))) 
              debugger.debug("set {} as child #{} of the parent {}"\
                .format(str(new_node), child_idx, str(parent))) 
              setChildren(parent, [new_node], [child_idx])
            num_rules_applied += 1
            if num_rules_applied > self.max_num_applications:
              # reaches the maximum limit
              debugger.debug("returned due to reaching max limit")
              return deepCopyAST(self.root)
            # if not finished, restarts the whole process from root
            iter = self.getNodeAndParentIter(self.root)
            break
      else:
        # all nodes have been traversed and no more rules can be applied
        debugger.debug("returned due to no more rules applicable")
        return deepCopyAST(self.root)
    # this method should never reach here 
    raise PlannerInternalError("'{}.findBestPlan()' reached somewhere invalid.".format(getClassNameOfInstance(self)))
    