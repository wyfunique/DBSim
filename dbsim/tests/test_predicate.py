import typing
from typing import *
from operator import eq
from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeAdapter, EmployeeDataFrameAdapter
from ..query_parser import parse, parse_statement
from ..ast import *
from ..utils.predicate_utils import PredicateUtils
from ..utils.visualizer import LogicalPlanViz 
from ..query import Query

dataset = ds.DataSet()
adapter = EmployeeDataFrameAdapter()
dataset.add_adapter(adapter)

def test_Predicate():
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
  pred = Predicate(ast.bool_op)
  assert pred.sources == set(["employees.employee_id", "employees_2.employee_id"])
  
  ast2 = parse_statement("select * from table where 1 = 1")
  assert Predicate(ast2.bool_op).getSources() == set()
  assert ast2.getPredicate() == Predicate(ast2.bool_op)

  query = Query(dataset, ast)
  resolved_ast = query.getPlan()
  assert pred.relatedTo(resolved_ast) 
  assert pred.relatedTo(resolved_ast.relation)
  assert pred.relatedTo(resolved_ast.relation.left)
  assert pred.relatedTo(resolved_ast.relation.right)
  assert not ast2.getPredicate().relatedTo(resolved_ast.relation)

def test_PredicateUtils():
  # test PredicateUtils._decorrelateAnd
  sql = """
  select * from employees, employees_2 
  where 
    employees.employee_id = employees_2.employee_id 
    AND 
    employees.employee_id > 1000 
    AND 
    employees_2.employee_id < 2000
    AND
    employees.employee_id < 3000 
  """
  plan = Query(dataset, parse_statement(sql)).getPlan()
  """
  The plan should look like:
    selection: employees.employee_id = employees_2.employee_id AND employees.employee_id > 1000 AND employees_2.employee_id < 2000 AND ...
        |
      join: true
       / \
   Relation  Relation   
  """
  selection = plan
  join = selection.relation
  decorrelated_sel_pred: Dict[Predicate, List[int]] = \
    PredicateUtils._decorrelateAnd(Predicate(selection.bool_op), [join.left, join.right])
  assert len(decorrelated_sel_pred) == 4
  for pred in decorrelated_sel_pred:
    if pred.equalToExprByStr(" employees.employee_id = employees_2.employee_id "):
      assert len(decorrelated_sel_pred[pred]) == 2
      assert decorrelated_sel_pred[pred] == [0, 1]
    elif pred.equalToExprByStr(" employees.employee_id > 1000  "):
      assert len(decorrelated_sel_pred[pred]) == 1
      assert decorrelated_sel_pred[pred] == [0]
    elif pred.equalToExprByStr(" employees_2.employee_id < 2000  "):
      assert len(decorrelated_sel_pred[pred]) == 1
      assert decorrelated_sel_pred[pred] == [1]
    elif pred.equalToExprByStr(" employees.employee_id < 3000  "):
      assert len(decorrelated_sel_pred[pred]) == 1
      assert decorrelated_sel_pred[pred] == [0]
    else:
      # should never reach here
      assert False

  # test PredicateUtils._groupDecorrelatedAnd
  grouped: Dict[typing.Tuple[int], Predicate] = PredicateUtils._groupDecorrelatedAnd(decorrelated_sel_pred)
  for related_node_indices in grouped:
    # related_nodes is a tuple of tuple - ((node_1, node_idx_1), (node_2, node_idx_2), (node_3, node_idx_3)...)
    if len(related_node_indices) == 2:
      assert grouped[related_node_indices].equalToExprByStr(" employees.employee_id = employees_2.employee_id ")
    elif len(related_node_indices) == 1:
      if related_node_indices[0] == 0:
        assert grouped[related_node_indices].equalToExprByStr(" employees.employee_id > 1000 AND employees.employee_id < 3000 ")
      elif related_node_indices[0] == 1:
        assert grouped[related_node_indices].equalToExprByStr(" employees_2.employee_id < 2000 ")
    else:
      # should never reach here
      assert False

  # test PredicateUtils.decorrelateAnd
  assert grouped == PredicateUtils.decorrelateAnd(Predicate(selection.bool_op), [join.left, join.right])

  # test auto de-duplicating in PredicateUtils._decorrelateAnd
  sql = """
  select * from employees 
  where 
    employees.employee_id = 1 AND employees.employee_id = 1 AND employees.employee_id = 1 
  """
  plan = Query(dataset, parse_statement(sql)).getPlan()
  selection = plan
  relation = selection.relation
  decorrelated_sel_pred = \
    PredicateUtils._decorrelateAnd(Predicate(selection.bool_op), [relation])
  assert len(decorrelated_sel_pred) == 1
  pred = list(decorrelated_sel_pred.keys())[0]
  assert pred.equalToExprByStr(" employees.employee_id = 1 ")

  # test the case that there are no related nodes or no AND expression
  sql = """
  select * from employees 
  where 
    employees.employee_id = 1 
  """
  plan = Query(dataset, parse_statement(sql)).getPlan()
  selection = plan
  relation = selection.relation
  decorrelated_sel_pred: Dict[typing.Tuple[int], Predicate] = \
    PredicateUtils.decorrelateAnd(Predicate(selection.bool_op), [relation])
  assert len(decorrelated_sel_pred) == 1
  pred = list(decorrelated_sel_pred.values())[0]
  assert pred.equalToExprByStr(" employees.employee_id = 1 ")

  sql = """
  select * from employees 
  where 
    1 = 1 
  """
  plan = Query(dataset, parse_statement(sql)).getPlan()
  selection = plan
  relation = selection.relation
  decorrelated_sel_pred: Dict[typing.Tuple[int], Predicate] = \
    PredicateUtils.decorrelateAnd(Predicate(selection.bool_op), [relation])
  assert len(decorrelated_sel_pred) == 1
  related_node_indices = list(decorrelated_sel_pred.keys())[0]
  assert related_node_indices == tuple()
  