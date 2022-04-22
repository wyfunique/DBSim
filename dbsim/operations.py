# functions for manipulating operations go here

from functools import partial
from zipper import zipper, Loc

from . import Relation
from .ast import LoadOp, EqOp, RelationalOp, Function, Var, BinRelationalOp, Expr
from typing import Callable

def query_zipper(operation: Expr) -> Loc:
  return zipper(operation, is_branch, children, make_node)

def is_branch(operation):
  return not type(operation) in (Relation, LoadOp)

def children(operation):
  if isinstance(operation, BinRelationalOp):
    return (operation.left, operation.right)
  elif isinstance(operation, Function):
    return tuple(
      arg 
      for arg in operation.args 
      if isinstance(arg, RelationalOp) or isinstance(arg, Function)
    )
  else:
    return (operation.relation,)

def make_node(operation, children):
  if isinstance(operation, BinRelationalOp):
    left, right = children
    return operation.new(left=left, right=right)
  elif isinstance(operation, Function):
    pos = 0
    args = []
    for arg in operation.args:
      if isinstance(arg, RelationalOp) or isinstance(arg, Function):
        args.append(children[pos])
        pos += 1
      else:
        args.append(arg)
    return operation.new(args=args)
  else:
    return operation.new(relation=children[0])




def walk(operation: Expr, visitor: Callable[[Loc], Loc]):
  loc = query_zipper(operation).leftmost_descendant()

  while True:
    loc = visitor(loc)
    if loc.at_end():
      # traversal has ended
      break
    else:
      # go to the next node by postorder
      loc = loc.postorder_next()

  return loc.root() 



def isa(type):
  def test(loc):
    return isinstance(loc.node(), type)
  return test

def is_not(type):
  def test(loc):
    return not isinstance(loc.node(), type)
  return test

def visit_with(dataset, *visitors):
  def visitor(loc):
    for cond,f in visitors:
      if cond(loc):
        loc = f(dataset, loc, loc.node())
    return loc
  return visitor

