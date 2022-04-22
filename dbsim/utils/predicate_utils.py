import typing
from typing import List, Set, Dict, Union
from copy import copy, deepcopy
from enum import Enum
from collections import namedtuple

from . import *
from .exceptions import *
from ..ast import *

class PredicateUtils(object):

  @classmethod
  def _isAnd(cls, predicate: Predicate) -> bool:
    """Returns True if the given predicate is an AND expression"""
    return isinstance(predicate.toExpr(), And)
  
  @classmethod
  def _decorrelateAnd(cls, predicate: Predicate, nodes: List[Expr]) -> Dict['Predicate', List[int]]:
    """
    Decorrelates an AND expression into one or more parts, 
      where each part is (to the best effort) only related to one of the given nodes.
    
    If dupilcated parts are found, they will be automatically de-duplicated in the returning results, 
      for example, "X = 1 AND X = 1" after decorrelation will become Dict('X = 1': list(...)).

    This method is critical for rules like predicate-push-down. 
    Specifically, when an expression part is only related to one lower node, it can be pushed down to that node;
      while an expression part related to multiple lower nodes cannot be pushed down.
    
    Note that only AND expression can be decorrelated for pushing down each part individually, 
      while OR expression has to be pushed down (if it can be) as a whole. 

    Parameters
    -----------
    nodes: a list of plan nodes to check relevance with

    Return
    -----------
    a dict where each key is a predicate and each value is the indices of the related nodes in the input list. 
    """
    def _findAllRelated(pred: Predicate, nodes: List[Expr]) -> List[int]:
      related_to = list()
      for i, node in enumerate(nodes):
        if pred.relatedTo(node):
          related_to.append(i)
      return related_to

    if cls._isAnd(predicate):
      pred_lhs, pred_rhs = Predicate(predicate.toExpr().lhs), Predicate(predicate.toExpr().rhs)
      decorrelated_lhs = cls._decorrelateAnd(pred_lhs, nodes)
      decorrelated_rhs = cls._decorrelateAnd(pred_rhs, nodes)
      part_to_related_nodes = decorrelated_lhs
      """
      Duplicated predicates will be automatically de-duplicated during the dict update.
      """
      part_to_related_nodes.update(decorrelated_rhs)
      return part_to_related_nodes
    else:
      # if this predicate is not an AND expression, we should not decorrelate it.
      return {predicate: _findAllRelated(predicate, nodes)}  

  @classmethod
  def _mergeByAnd(cls, left_pred: 'Predicate', right_pred: 'Predicate') -> Predicate:
    return Predicate(And(left_pred.toExpr(), right_pred.toExpr()))

  @classmethod
  def _groupDecorrelatedAnd(cls, decorrelatedAnd: Dict['Predicate', List[int]])\
        -> Dict[typing.Tuple[int], 'Predicate']:
    """
    Groups the results returned by self._decorrelateAnd by the related node.

    For example, say there is a JoinOp X, 
      if expression A is related to X.left, expression B is also related to X.left, 
      then this method will merge the two expressions into a "A AND B" expression, 
      and its corresponding entry in the resulting dict will be "indexOf(X.left): A AND B".
    """
    groups = dict()
    for pred in decorrelatedAnd:
      related_node_indices = tuple(decorrelatedAnd[pred])
      if related_node_indices in groups:
        groups[related_node_indices] = cls._mergeByAnd(groups[related_node_indices], pred)
      else:
        groups[related_node_indices] = pred
    return groups
  
  @classmethod
  def decorrelateAnd(cls, predicate: Predicate, nodes: List[Expr]) -> Dict[typing.Tuple[int], 'Predicate']:
    """
    A wrapper for _decorrelateAnd + _groupDecorrelatedAnd,
      i.e., first decorrelating And predicate, then grouping the results and returning.
    """
    return cls._groupDecorrelatedAnd(cls._decorrelateAnd(predicate, nodes))