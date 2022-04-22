from operator import eq
from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeDataFrameAdapter, EmployeeVectorAdapter
from ..query_parser import parse, parse_statement
from ..query import Query
from ..ast import *
from ..utils.visualizer import LogicalPlanViz 
from ..utils.exceptions import *
from ..planners import rules
from ..planners import planner
from ..planners.heuristic.heuristic_planner import HeuristicPlanner
from ..compilers import local

dataset = ds.DataSet()
adapter = EmployeeDataFrameAdapter()
dataset.add_adapter(adapter)
dataset.add_adapter(EmployeeVectorAdapter())

def test_addRule():
  rule1 = rules.FilterMergeRule()
  rule2 = rules.FilterMergeRule()
  optim = planner.Planner()
  success = optim.addRule(rule1)
  assert success
  failure = optim.addRule(rule2)
  assert not failure 

def test_HeuristicPlanner_one_rule_one_match():
  """
  Only one rule added and only one match exists between the plan and rule
  """
  rule1 = rules.FilterMergeRule()
  planner = HeuristicPlanner(max_limit = float('Inf'))
  planner.addRule(rule1)
  assert len(planner.rule_set) == len(planner.rule_seq)
  assert len(planner.rule_seq) == 1
  sql = """
    select employees_2.employee_id
    from 
      (select * from employees, employees_2 where employees.employee_id = employees_2.employee_id)
    where employees.employee_id = 1234
  """
  plan = parse_statement(sql)
  assert not rule1.matches(plan)
  assert rule1.matches(plan.relation)
  #LogicalPlanViz.show(plan)
  """
  The plan should look like:
    projection: employees_2.employee_id
        |
    selection: employees.employee_id = 1234
        |
    selection: employees.employee_id = employees_2.employee_id
        |              
      join: true
    /             \   
load: employees  load: employees_2
  """
  planner.setRoot(plan)
  try:
    equiv_subplans = planner.applyRule(plan.relation, rule1)
    # planner.applyRule requires resolved plan, while plan.relation is an unresolved expression tree, 
    # so this line should always throw exception, i.e., it should never reach 'assert False'. 
    assert False 
  except Exception as e:
    # planner.applyRule should throw a PlannerInternalError exception
    assert isinstance(e, PlannerInternalError)

  resolved_plan = Query(dataset, plan).operations
  new_subplan = planner.applyRule(resolved_plan.relation, rule1)
  assert not rule1.matches(new_subplan)
  #LogicalPlanViz.show(new_subplan)
  """
  The new_subplan should look like:
    selection: employees.employee_id = employees_2.employee_id and employees.employee_id = 1234
        |              
      join: true
    /             \   
Relation: employees  Relation: employees_2
  """
  best_plan = planner.findBestPlan(resolved_plan)
  #LogicalPlanViz.show(best_plan)
  """
  The best_plan should look like:
    projection: employees_2.employee_id
        |
    selection: employees.employee_id = employees_2.employee_id and employees.employee_id = 1234
        |              
      join: true
    /             \   
Relation: employees  Relation: employees_2
  """
  assert str(best_plan) == str(plan)

  best_query = planner.optimizeQuery(Query(dataset, plan))
  assert str(best_query.operations) == str(best_plan)
  assert best_plan.equal(best_query.operations, ignore_schema=False, match_loadop_and_relation=False)
  assert best_query.getPlan() is best_query.operations

def test_HeuristicPlanner_one_rule_multi_matches():
  """
  Only one rule added and multiple matches exist between the plan and rule
  """
  rule1 = rules.FilterMergeRule()
  planner = HeuristicPlanner(max_limit = float('Inf'))
  planner.addRule(rule1)
  assert len(planner.rule_set) == len(planner.rule_seq)
  assert len(planner.rule_seq) == 1
  
  sql = """
    select employees_2.employee_id
    from 
    (
      select *
      from 
        (select * from employees, employees_2 where employees.employee_id = employees_2.employee_id)
      where employees.employee_id = 1234
    )
    where employees.employee_id > 1000
  """
  plan = parse_statement(sql)
  # two matches exist
  """
  The plan should look like:
    projection: employees_2.employee_id
        |
    selection: employees.employee_id > 1000
        |
    selection: employees.employee_id = 1234
        |
    selection: employees.employee_id = employees_2.employee_id
        |              
      join: true
    /             \   
load: employees  load: employees_2
  """
  #LogicalPlanViz.show(plan)
  best_plan = planner.optimizeQuery(Query(dataset, plan)).getPlan()
  assert str(best_plan) == str(plan)
  #LogicalPlanViz.show(best_plan)
  """
  The best_plan should look like:
    projection: employees_2.employee_id
        |
    selection: employees.employee_id = employees_2.employee_id and employees.employee_id = 1234 and employees.employee_id > 1000
        |              
      join: true
    /             \   
Relation: employees  Relation: employees_2
  """

def test_HeuristicPlanner_multi_rules_multi_matches():
  """
  Multiple rules added and multiple matches exist between the plan and rules
  """
  sql = """
    select employees_2.employee_id
    from 
      (select * from employees, employees_2 where employees.employee_id = employees_2.employee_id)
    where employees.employee_id = 1234
  """
  query = Query(dataset, parse_statement(sql))
  plan = query.getPlan()
  """
  The plan should look like:
    projection: employees_2.employee_id
          |
    selection: employees.employee_id = 1234
          |
    selection: employees.employee_id = employees_2.employee_id
          |              
        join: true
    /                 \   
Relation: employees  Relation: employees_2
  """
  planner = HeuristicPlanner(max_limit = float('Inf'))
  planner.addRule(rules.FilterMergeRule())
  planner.addRule(rules.FilterPushDownRule())
  best_plan = planner.findBestPlan(plan)
  """
  The best_plan should look like:
    projection: employees_2.employee_id
                |
    selection: employees.employee_id = employees_2.employee_id 
                |              
            join: true
    /                         \   
selection:                    Relation: employees_2
employees.employee_id = 1234
    |
Relation: employees  
  """
  assert isinstance(best_plan, ProjectionOp)
  assert isinstance(best_plan.relation, SelectionOp)\
    and Predicate.fromRelationOp(best_plan.relation)\
       .equalToExprByStr("employees.employee_id = employees_2.employee_id ") 
  assert isinstance(best_plan.relation.relation, JoinOp)\
    and Predicate.fromRelationOp(best_plan.relation.relation)\
       .equalToExprByStr("true") 
  assert isinstance(best_plan.relation.relation.left, SelectionOp)\
     and Predicate.fromRelationOp(best_plan.relation.relation.left)\
       .equalToExprByStr("employees.employee_id = 1234") 
  assert isinstance(best_plan.relation.relation.right, Relation)
  assert isinstance(best_plan.relation.relation.left.relation, Relation)

  assert Query(dataset, plan, resolve_op_schema=False).get_pretty_results() ==\
         Query(dataset, best_plan, resolve_op_schema=False).get_pretty_results()

  # test on a different rule order from above
  planner = HeuristicPlanner(max_limit = float('Inf'))
  planner.addRule(rules.FilterPushDownRule())
  planner.addRule(rules.FilterMergeRule())
  best_plan = planner.findBestPlan(plan)
  """
  The best_plan should look like the same as above:
    projection: employees_2.employee_id
                |
    selection: employees.employee_id = employees_2.employee_id 
                |              
            join: true
    /                         \   
selection:                    Relation: employees_2
employees.employee_id = 1234
    |
Relation: employees  
  """
  assert isinstance(best_plan, ProjectionOp)
  assert isinstance(best_plan.relation, SelectionOp)\
    and Predicate.fromRelationOp(best_plan.relation)\
       .equalToExprByStr("employees.employee_id = employees_2.employee_id ") 
  assert isinstance(best_plan.relation.relation, JoinOp)\
    and Predicate.fromRelationOp(best_plan.relation.relation)\
       .equalToExprByStr("true") 
  assert isinstance(best_plan.relation.relation.left, SelectionOp)\
     and Predicate.fromRelationOp(best_plan.relation.relation.left)\
       .equalToExprByStr("employees.employee_id = 1234") 
  assert isinstance(best_plan.relation.relation.right, Relation)
  assert isinstance(best_plan.relation.relation.left.relation, Relation)

  assert Query(dataset, plan, resolve_op_schema=False).get_pretty_results() ==\
         Query(dataset, best_plan, resolve_op_schema=False).get_pretty_results()


def test_HeuristicPlanner_multi_rules_multi_matches_with_simselect():
  """
  Multiple rules added and multiple matches exist between the plan and rules, 
  including simselect and Selection_SimSelection_Swap_Rule
  """
  sql = """
    select employees_with_vectors.employee_id
    from 
      (select * 
        from 
          (select * from employees, employees_with_vectors where employees.employee_id = employees_with_vectors.employee_id) 
        where 
          employees_with_vectors.vector to [1,2,3,4] < 10
      )
    where employees.employee_id > 1500
  """
  query = Query(dataset, parse_statement(sql))
  plan = query.getPlan()
  #LogicalPlanViz.show(plan)

  """
  The plan should look like:
    projection: employees_with_vectors.employee_id
          |
    selection: employees.employee_id > 1500
          |
    simselection: employees_with_vectors.vector to [1,2,3,4] < 10
          |
    selection: employees.employee_id = employees_with_vectors.employee_id
          |              
        join: true
    /                 \   
Relation: employees  Relation: employees_with_vectors
  """
  planner = HeuristicPlanner(max_limit = float('Inf'))
  planner.addRule(rules.FilterMergeRule())
  planner.addRule(rules.FilterPushDownRule())
  planner.addRule(rules.Selection_SimSelection_Swap_Rule())
  best_plan = planner.findBestPlan(plan)
  """
  The best_plan should look like:
    projection: employees_with_vectors.employee_id
                |
    simselection: employees_with_vectors.vector to [1,2,3,4] < 10
                |
    selection: employees.employee_id = employees_with_vectors.employee_id 
                |              
            join: true
    /                         \   
selection:                    Relation: employees_with_vectors
employees.employee_id > 1500
    |
Relation: employees  
  """
  #LogicalPlanViz.show(best_plan)
  assert getClassNameOfInstance(best_plan) == 'ProjectionOp'
  assert getClassNameOfInstance(best_plan.relation) == 'SimSelectionOp'
  assert getClassNameOfInstance(best_plan.relation.relation) == 'SelectionOp'\
    and Predicate.fromRelationOp(best_plan.relation.relation)\
       .equalToExprByStr("employees.employee_id = employees_with_vectors.employee_id ") 
  assert getClassNameOfInstance(best_plan.relation.relation.relation) == 'JoinOp'\
    and Predicate.fromRelationOp(best_plan.relation.relation.relation)\
       .equalToExprByStr("true") 
  assert getClassNameOfInstance(best_plan.relation.relation.relation.left) == 'SelectionOp'\
     and Predicate.fromRelationOp(best_plan.relation.relation.relation.left)\
       .equalToExprByStr("employees.employee_id > 1500") 
  assert isinstance(best_plan.relation.relation.relation.right, Relation)
  assert isinstance(best_plan.relation.relation.relation.left.relation, Relation)

  assert Query(dataset, plan, resolve_op_schema=False).get_pretty_results() ==\
         Query(dataset, best_plan, resolve_op_schema=False).get_pretty_results()

def test_integrated_optimization_in_query():
  planner = HeuristicPlanner(max_limit = float('Inf'))
  planner.addRule(rules.FilterMergeRule())
  planner.addRule(rules.FilterPushDownRule())
  planner.addRule(rules.Selection_SimSelection_Swap_Rule())
  sql = """
    select employees_with_vectors.employee_id
    from 
      (select * 
        from 
          (select * from employees, employees_with_vectors where employees.employee_id = employees_with_vectors.employee_id) 
        where 
          employees_with_vectors.vector to [1,2,3,4] < 10
      )
    where employees.employee_id > 1500
  """
  query = Query(dataset, parse_statement(sql))
  plan = query.getPlan()
  best_plan = planner.findBestPlan(plan)
  auto_optimized_query = Query(dataset, parse_statement(sql), resolve_op_schema=True, optimizer=planner)
  assert best_plan.equal(auto_optimized_query.getPlan(), ignore_schema=False, match_loadop_and_relation=False)\
     and not plan.equal(auto_optimized_query.getPlan(), ignore_schema=False, match_loadop_and_relation=False)

  empty_planner = HeuristicPlanner(max_limit = float('Inf'))
  non_optimized_query = Query(dataset, parse_statement(sql), resolve_op_schema=True, optimizer=empty_planner)
  assert plan.equal(non_optimized_query.getPlan(), ignore_schema=False, match_loadop_and_relation=False)\
     and not best_plan.equal(non_optimized_query.getPlan(), ignore_schema=False, match_loadop_and_relation=False)

  assert auto_optimized_query.get_pretty_results() ==\
         non_optimized_query.get_pretty_results() \
        and Query(dataset, best_plan, resolve_op_schema=False).get_pretty_results() ==\
          Query(dataset, plan, resolve_op_schema=False).get_pretty_results() \
        and auto_optimized_query.get_pretty_results() ==\
          Query(dataset, plan, resolve_op_schema=False).get_pretty_results()