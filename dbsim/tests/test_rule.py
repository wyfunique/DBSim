from dbsim.extensions.extended_syntax.sim_select_syntax import SimSelectionOp
from dbsim.query import Query
from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeDataFrameAdapter, EmployeeVectorAdapter
from ..query_parser import parse, parse_statement
from ..ast import *
from ..utils.visualizer import LogicalPlanViz 
from ..planners import rules

dataset = ds.DataSet()
adapter = EmployeeDataFrameAdapter()
dataset.add_adapter(adapter)

dataset.add_adapter(EmployeeVectorAdapter())

def test_FilterMergeRule():
  sql = """
    select employees_2.employee_id
    from 
      (select * from employees, employees_2 where employees.employee_id = employees_2.employee_id)
    where employees.employee_id = 1234
  """
  ast = parse_statement(sql)
  query = Query(dataset, ast)
  resolved_plan = query.operations

  # Query.__init__(dataset, operations) will replace LoadOp with the source Relation, 
  # so here 'ast' includes LoadOp while resolved_plan does not. 
  assert equalExprs(ast, resolved_plan, ignore_schema=True, match_loadop_and_relation=True)
  #LogicalPlanViz.show(ast)
  """
  The ast (which is unresolved) should look like:
    projection: employee_id
        |
    selection: employee_id = 1234
        |
    selection: employees.employee_id = employees_2.employee_id
        |              
      join: true
    /             \   
load: employees  load: employees_2
  """
  rule = rules.FilterMergeRule()
  assert not rule.matches(ast)
  assert rule.matches(ast.relation)

  try:
    equiv_subplans = rule.transform(ast.relation, inplace = False)
    # rule.transform requires resolved plan, while ast.relation is an unresolved ast tree, 
    # so this line should always throw exception, i.e., it should never reach 'assert False'. 
    assert False 
  except Exception as e:
    # rule.transform should throw a ValueError exception
    assert isinstance(e, ValueError)

  ast = resolved_plan
  ast_original = [node for node in traverse(ast)]  
  equiv_plan = deepcopy(ast)

  equiv_subplans = rule.transform(ast.relation, inplace = False)
  assert isinstance(equiv_subplans, list)
  assert len(equiv_subplans) == 1
  setChildren(equiv_plan, equiv_subplans, [0])
  ast.relation = rule.transform(ast.relation, inplace = True)
  # remove the second node (i.e., the upper selection) from the original node sequence as it will be merged into the lower selection, 
  #   i.e., this node is impossible to keep original 
  ast_original = ast_original[:1] + ast_original[2:]
  """
  The equiv_plan (which is resolved) should look like:
    projection: employee_id
        |
    selection: employees.employee_id = employees_2.employee_id and employee_id = 1234
        |              
      join: true
    /             \   
Relation: employees  Relation: employees_2
  """
  truth_node_class_seq = [ProjectionOp, SelectionOp, JoinOp, Relation, Relation]
  ast_node_seq = [node for node in traverse(ast)]
  equiv_node_seq = [node for node in traverse(equiv_plan)]
  for i in range(len(ast_node_seq)):
    assert ast_node_seq[i] == equiv_node_seq[i]
    assert truth_node_class_seq[i] == ast_node_seq[i].__class__
    assert truth_node_class_seq[i] == equiv_node_seq[i].__class__
    assert id(ast_node_seq[i]) != id(equiv_node_seq[i])      
    if i == 1:
      assert equalPredicate(
        ast_node_seq[i].bool_op, 
        "employees.employee_id = employees_2.employee_id and employees.employee_id = 1234"
      )
      assert equalPredicate(
        equiv_node_seq[i].bool_op, 
        "employees.employee_id = employees_2.employee_id and employees.employee_id = 1234"
      )
    if i == 0 or i >= 2:
      # these nodes in ast should be the original nodes in ast, instead of copy, as ast was transformed inplace. 
      assert id(ast_original[i]) == id(ast_node_seq[i])
      # while these nodes in equiv_plan should be deep copy of the corresponding ast nodes.
      assert id(ast_original[i]) != id(equiv_node_seq[i])

  # finally, the transformed plan should generate the same results as original plan after execution
  query1 = Query(dataset, resolved_plan, resolve_op_schema=False)
  query2 = Query(dataset, equiv_plan, resolve_op_schema=False)
  assert query1.get_pretty_results() == query2.get_pretty_results() 


def test_FilterPushDownRule():
  sql = """
    select * from employees, employees_2 
    where employees.employee_id > 1234 AND employees.employee_id = employees_2.employee_id
  """
  ast = parse_statement(sql)
  query = Query(dataset, ast)
  resolved_plan = query.operations
  #LogicalPlanViz.show(ast)
  """
  The resolved_plan should look like:
    selection: employees.employee_id > 1234 AND employees.employee_id = employees_2.employee_id
        |              
      join: true
    /             \   
Relation: employees  Relation: employees_2
  """
  rule = rules.FilterPushDownRule()
  assert rule.matches(ast)
  assert rule.matches(resolved_plan)

  try:
    rule.transform(ast, inplace = False)
    # rule.transform requires resolved plan, while ast.relation is an unresolved ast tree, 
    # so this line should always throw exception, i.e., it should never reach 'assert False'. 
    assert False 
  except Exception as e:
    # rule.transform should throw a ValueError exception
    assert isinstance(e, ValueError)

  assert isinstance(resolved_plan, SelectionOp)\
     and Predicate.fromRelationOp(resolved_plan)\
       .equalToExprByStr("employees.employee_id > 1234 AND employees.employee_id = employees_2.employee_id") 
  assert isinstance(resolved_plan.relation, JoinOp)\
     and Predicate.fromRelationOp(resolved_plan.relation)\
       .equalToExprByStr("true")
  assert isinstance(resolved_plan.relation.left, Relation) and isinstance(resolved_plan.relation.right, Relation)

  equiv_plan = rule.transform(resolved_plan)[0] 
  """
  The transformed plan should look like:
    selection: employees.employee_id = employees_2.employee_id
              |              
          join: true
      /               \
selection:            Relation: employees_2
employees.employee_id > 1234
    /                
Relation: employees  
  """
  assert isinstance(equiv_plan, SelectionOp)\
     and Predicate.fromRelationOp(equiv_plan)\
       .equalToExprByStr("employees.employee_id = employees_2.employee_id") 
  assert isinstance(equiv_plan.relation, JoinOp)\
     and Predicate.fromRelationOp(equiv_plan.relation)\
       .equalToExprByStr("true")
  assert isinstance(equiv_plan.relation.left, SelectionOp)\
     and Predicate.fromRelationOp(equiv_plan.relation.left)\
       .equalToExprByStr("employees.employee_id > 1234") 
  assert isinstance(equiv_plan.relation.right, Relation)
  assert isinstance(equiv_plan.relation.left.relation, Relation)

  assert Query(dataset, resolved_plan, resolve_op_schema=False).get_pretty_results() ==\
         Query(dataset, equiv_plan, resolve_op_schema=False).get_pretty_results()
  
  # test the case that all selection sub-predicates are pushed down 
  sql = """
    select * from employees join employees_2 on employees.employee_id = employees_2.employee_id
    where employees.employee_id > 1234 
  """
  ast = parse_statement(sql)
  query = Query(dataset, ast)
  resolved_plan = query.operations
  #LogicalPlanViz.show(ast)
  """
  The resolved_plan should look like:
    selection: employees.employee_id > 1234
        |              
      join: employees.employee_id = employees_2.employee_id
    /             \   
Relation: employees  Relation: employees_2
  """
  equiv_plan = rule.transform(resolved_plan)[0] 
  """
  The transformed plan should look like:          
          join: employees.employee_id = employees_2.employee_id
      /               \
selection:            Relation: employees_2
employees.employee_id > 1234
    /                
Relation: employees  
  """
  assert isinstance(equiv_plan, JoinOp)\
     and Predicate.fromRelationOp(equiv_plan)\
       .equalToExprByStr("employees.employee_id = employees_2.employee_id") 
  assert isinstance(equiv_plan.left, SelectionOp)\
     and Predicate.fromRelationOp(equiv_plan.left)\
       .equalToExprByStr("employees.employee_id > 1234") 
  assert isinstance(equiv_plan.right, Relation)
  assert isinstance(equiv_plan.left.relation, Relation)

  assert Query(dataset, resolved_plan, resolve_op_schema=False).get_pretty_results() ==\
         Query(dataset, equiv_plan, resolve_op_schema=False).get_pretty_results()
  
  # test the case that all selection sub-predicates are pushed down (case #2)
  sql = """
    select * from employees join employees_2 on employees.employee_id = employees_2.employee_id
    where employees.employee_id > 1234 AND employees_2.employee_id > 1000 AND employees.employee_id < 10000 
  """
  ast = parse_statement(sql)
  query = Query(dataset, ast)
  resolved_plan = query.operations
  #LogicalPlanViz.show(ast)
  """
  The resolved_plan should look like:
    selection: employees.employee_id > 1234 AND employees_2.employee_id > 1000 AND employees.employee_id < 10000 
        |              
      join: employees.employee_id = employees_2.employee_id
    /             \   
Relation: employees  Relation: employees_2
  """
  equiv_plan = rule.transform(resolved_plan)[0] 
  """
  The transformed plan should look like:     

          join: employees.employee_id = employees_2.employee_id
      /                                                         \
selection:                                                  selection:   
employees.employee_id > 1234                            employees_2.employee_id > 1000
AND 
employees.employee_id < 10000                                       \
    /                
Relation: employees                                         Relation: employees_2
  """
  assert isinstance(equiv_plan, JoinOp)\
     and Predicate.fromRelationOp(equiv_plan)\
       .equalToExprByStr("employees.employee_id = employees_2.employee_id") 
  assert isinstance(equiv_plan.left, SelectionOp)\
     and Predicate.fromRelationOp(equiv_plan.left)\
       .equalToExprByStr("employees.employee_id > 1234 AND employees.employee_id < 10000") 
  assert isinstance(equiv_plan.right, SelectionOp)\
     and Predicate.fromRelationOp(equiv_plan.right)\
       .equalToExprByStr("employees_2.employee_id > 1000") 
  assert isinstance(equiv_plan.left.relation, Relation)
  assert isinstance(equiv_plan.right.relation, Relation)

  assert Query(dataset, resolved_plan, resolve_op_schema=False).get_pretty_results() ==\
         Query(dataset, equiv_plan, resolve_op_schema=False).get_pretty_results()


def test_SimSel_Sel_Swap_Rule():
  query_vec = [1, 2, 3, 4]
  sql = """
    select employees_with_vectors.employee_id
    from 
      (select * from employees_with_vectors where employees_with_vectors.vector to {} < 10)
    where employees_with_vectors.employee_id > 1500
  """.format(query_vec)
  assert sql
  ast = parse_statement(sql)
  query = Query(dataset, ast)
  resolved_plan = query.operations
  """
  The ast (which is unresolved) should look like:
    projection: employee_id
        |
    selection: employee_id > 1500
        |
    simselection: vector to [1,2,3,4] < 10
        |              
    load: employees_with_vectors
  """
  rule = rules.Selection_SimSelection_Swap_Rule()
  assert not rule.matches(ast)
  assert rule.matches(ast.relation)
  #LogicalPlanViz.show(ast)
  equiv_plan = deepCopyAST(resolved_plan)
  equiv_plan.relation = rule.transform(resolved_plan.relation)[0]
  """
  The equiv_plan (which is resolved) should look like:
    projection: employee_id
        |
    simselection: vector to [1,2,3,4] < 10
        |
    selection: employee_id > 1500
        |              
    Relation: employees_with_vectors
  """
  assert getClassNameOfInstance(equiv_plan.relation) == 'SimSelectionOp' \
        and getClassNameOfInstance(equiv_plan.relation.relation) == 'SelectionOp' 
  #LogicalPlanViz.show(equiv_plan)
        

  
