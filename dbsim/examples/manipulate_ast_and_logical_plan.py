import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from dbsim import dataset as ds
from dbsim.tests.fixtures.demo_adapter import DemoAdapter
from dbsim.query_parser import parse_statement
from dbsim.query import Query
from dbsim.planners import rules
from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
from dbsim.utils.visualizer import LogicalPlanViz 
from dbsim.ast import *

dataset = ds.DataSet()
dataset.add_adapter(DemoAdapter())

planner = HeuristicPlanner(max_limit = float('Inf'))
planner.addRule(rules.FilterMergeRule())
planner.addRule(rules.FilterPushDownRule())
planner.addRule(rules.Selection_SimSelection_Swap_Rule())

standard_sql = """
    SELECT musical.title, musical.year
    FROM 
      (SELECT * 
        FROM 
          (animation JOIN musical ON animation.mid = musical.mid) 
        WHERE 
          animation.mid < 1200
      )
    WHERE musical.year > 1960
"""
ast = parse_statement(standard_sql)
plan = Query(dataset, parse_statement(standard_sql)).getPlan()
best_plan = planner.findBestPlan(plan)

"""
Check if two ASTs/plans are identical 
"""
# check if two logical plans are identical
plan_2 = Query(dataset, parse_statement(standard_sql)).getPlan()
# comparing them including their schema info
assert plan.equal(plan_2, ignore_schema=False)
# comparing the plan tree structure only without considering their schema info
assert plan.equal(plan_2, ignore_schema=True)
# plan should be different from best_plan
assert not plan.equal(best_plan, ignore_schema=False)
# try the negation of .equal method -- .notEqual
assert plan.notEqual(best_plan, ignore_schema=False)

# check if two AST are identical
ast_2 = parse_statement("select 1")
# AST has no schema info, so whether to ignore the schema makes no difference
assert not ast.equal(ast_2)

# check if an AST and a logical plan are identical on tree structure.
# since ast has no schema but plan has, ignore_schema must be True,
#   and another parameter 'match_loadop_and_relation' must be True, 
#   which will consider LoadOp (only appearing in AST) and Relation (only appearing in logical plan) 
#   to be identical operators. 
assert ast.equal(plan, ignore_schema=True, match_loadop_and_relation=True)
# if either ignore_schema or match_loadop_and_relation is False, ast and plan are not identical
assert not ast.equal(plan, ignore_schema=True, match_loadop_and_relation=False)
assert not ast.equal(plan, ignore_schema=False, match_loadop_and_relation=True)

"""
Transform an AST into logical plan (by adding schema info into it)
"""
# Constructing a Query object with resolve_op_schema=True 
#   will resolve the schema of each tree node 
#   by which an AST is converted into logical plan.
plan_from_ast = Query(dataset, ast, resolve_op_schema=True).getPlan()
assert not ast.isResolved() and plan_from_ast.isResolved()
assert plan_from_ast.equal(ast, ignore_schema=True, match_loadop_and_relation=True)
assert not plan_from_ast.equal(ast, ignore_schema=False, match_loadop_and_relation=True)
# When resolve_op_schema=False, the constructor of Query will not resolve the schema 
#   but simply store a deepcopy of the original input AST/plan.
plan_from_plan = Query(dataset, plan_from_ast, resolve_op_schema=False).getPlan()
assert plan_from_plan.isResolved()
assert plan_from_plan is not plan_from_ast
assert plan_from_plan.equal(ast, ignore_schema=True, match_loadop_and_relation=True)
assert plan_from_plan.equal(plan_from_ast, ignore_schema=False, match_loadop_and_relation=False)

"""
Deep copy an AST/plan
"""
# deep copy an AST
ast_copy = deepCopyAST(ast)
assert ast_copy.equal(ast) and ast_copy is not ast
# deep copy a plan (including schema)
plan_copy = deepCopyAST(plan)
assert plan_copy.equal(plan, ignore_schema=False) and plan_copy is not plan

"""
Traverse an AST/plan by DFS or BFS
"""
print("Traversal by DFS ---> ")
for node in traverse(ast, order='dfs'):
  print(node)
print("Traversal by BFS ---> ")
for node in traverse(ast, order='bfs'):
  print(node)

"""
Get children of an AST/plan node
"""
print("Children of ast: ", getChildren(ast))
print("Children of best_plan: ", getChildren(best_plan))
