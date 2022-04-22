import typing
from typing import *
from operator import eq
from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeAdapter, EmployeeDataFrameAdapter
from ..query_parser import parse, parse_statement
from ..ast import *
from ..utils.visualizer import LogicalPlanViz 
from ..query import Query

dataset = ds.DataSet()
adapter = EmployeeDataFrameAdapter()
dataset.add_adapter(adapter)

def test_getChildren():
  sql = "select * from employees, employees_2 where employees.employee_id = employees_2.employee_id"
  ast = parse_statement(sql)
  """
  The ast should look like:
    selection: employees.employee_id = employees_2.employee_id
        |
      join: true
       / \
   load  load   
  """
  child1 = getChildren(ast)
  assert len(child1) == 1
  assert isinstance(child1[0], JoinOp)
  child2 = getChildren(child1[0])
  assert len(child2) == 2
  assert isinstance(child2[0], LoadOp)
  assert isinstance(child2[1], LoadOp)
  child3 = getChildren(child2[0])
  assert len(child3) == 0
  
def test_equalExprs_all_cases():
  sql = "select * from employees, employees_2 where employees.employee_id = employees_2.employee_id"
  ast = parse_statement(sql)
  ast2 = parse_statement(sql)
  assert equalUnresolvedExprs(ast, ast2)
  query1 = Query(dataset, ast)
  query2 = Query(dataset, ast2)
  assert equalResolvedExprs(query1.operations, query2.operations)
  sql2 = "select * from employees join employees_2 on employees.employee_id = employees_2.employee_id"
  ast3 = parse_statement(sql2)
  """
  The ast3 should look like:
      join: employees.employee_id = employees_2.employee_id
       / \
   load  load   
  """
  assert not equalUnresolvedExprs(ast, ast3)
  query3 = Query(dataset, ast3)
  assert not equalResolvedExprs(query1.operations, query3.operations)
  
  assert equalExprs(ast, ast2)
  assert equalExprs(ast, query2.operations, ignore_schema=True, match_loadop_and_relation=True)
  assert not equalExprs(ast, query2.operations, ignore_schema=False, match_loadop_and_relation=True)
  assert not equalExprs(ast, query2.operations, ignore_schema=True, match_loadop_and_relation=False)
  assert not equalExprs(ast, query2.operations, ignore_schema=False, match_loadop_and_relation=False)

def test_traverse():
  sql = """
    select * 
    from 
      (select employee_id from employees, employees_2 where employees.employee_id = employees_2.employee_id) as table1, 
      (select employee_id from employees, employees_2 where employees.employee_id = employees_2.employee_id) as table2
    where 
      table1.employee_id = table2.employee_id
  """
  ast = parse_statement(sql)
  #LogicalPlanViz.show(ast)
  """
  The ast should look like:
    selection: table1.employee_id = table2.employee_id
                 |
               join: true
       /                     \ 
    alias                   alias
      |                        |
  projection             projection: employee_id
      |                        |
  selection              selection: employees.employee_id = employees_2.employee_id
      |                        |
  join: true                join: true
   /      \                 /        \   
 load     load    load: employees  load: employees_2
  """
  i = 0
  dfs_truth_node_seq = [
    SelectionOp, 
    JoinOp, 
    AliasOp, ProjectionOp, SelectionOp, JoinOp, LoadOp, LoadOp, 
    AliasOp, ProjectionOp, SelectionOp, JoinOp, LoadOp, LoadOp
  ]
  dfs_truth_node_parent_seq = [
    (None, 0), 
    (SelectionOp, 0), 
    (JoinOp, 0), 
    (AliasOp, 0), (ProjectionOp, 0), (SelectionOp, 0), (JoinOp, 0), (JoinOp, 1), 
    (JoinOp, 1), (AliasOp, 0), (ProjectionOp, 0), (SelectionOp, 0), (JoinOp, 0), (JoinOp, 1)
  ]
  bfs_truth_node_seq = [
    SelectionOp, 
    JoinOp, 
    AliasOp, AliasOp, 
    ProjectionOp, ProjectionOp, 
    SelectionOp, SelectionOp, 
    JoinOp, JoinOp, 
    LoadOp, LoadOp, LoadOp, LoadOp
  ]
  bfs_truth_node_parent_seq = [
    (None, 0), 
    (SelectionOp, 0), 
    (JoinOp, 0), (JoinOp, 1), 
    (AliasOp, 0), (AliasOp, 0), 
    (ProjectionOp,  0), (ProjectionOp, 0), 
    (SelectionOp, 0), (SelectionOp, 0), 
    (JoinOp, 0), (JoinOp, 1), (JoinOp, 0), (JoinOp, 1)
  ]
  for node in traverse(ast, 'dfs'):
    assert isinstance(node, dfs_truth_node_seq[i])
    i += 1
  i = 0
  for node in traverse(ast, 'bfs'):
    assert isinstance(node, bfs_truth_node_seq[i])
    i += 1
  i = 0
  for node, parent, idx in traverseWithParent(ast, None, 0, 'dfs'):
    assert isinstance(node, dfs_truth_node_seq[i]) 
    if i == 0:
      assert parent is None
    else:
      assert isinstance(parent, dfs_truth_node_parent_seq[i][0]) and idx == dfs_truth_node_parent_seq[i][1]
    i += 1
  i = 0
  for node, parent, idx in traverseWithParent(ast, None, 0, 'bfs'):
    assert isinstance(node, bfs_truth_node_seq[i])
    if i == 0:
      assert parent is None
    else:
      assert isinstance(parent, bfs_truth_node_parent_seq[i][0]) and idx == bfs_truth_node_parent_seq[i][1]
    i += 1
  
def test_deepcopyAST():
  sql = """
    select * 
    from 
      (select employee_id from employees, employees_2 where employees.employee_id = employees_2.employee_id) as table1, 
      (select employee_id from employees, employees_2 where employees.employee_id = employees_2.employee_id) as table2
    where 
      table1.employee_id = table2.employee_id
  """
  ast = parse_statement(sql)
  copy_ast = deepCopyAST(ast)
  ast_node_seq = [node for node in traverse(ast, 'dfs')]
  copy_ast_node_seq = [node for node in traverse(copy_ast, 'dfs')]
  assert len(ast_node_seq) == len(copy_ast_node_seq)
  for i in range(len(ast_node_seq)):
    assert isinstance(copy_ast_node_seq[i], ast_node_seq[i].__class__)
    assert id(copy_ast_node_seq[i]) != id(ast_node_seq[i])

