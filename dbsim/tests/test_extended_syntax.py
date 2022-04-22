from dbsim.query import Query
from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeDataFrameAdapter, EmployeeVectorAdapter
from .fixtures.spatial_adapter import SpatialAdapter
from ..query_parser import parse, parse_statement
from ..ast import *
from ..utils.visualizer import LogicalPlanViz 
from ..planners import rules
from ..extensions.extended_syntax.sim_select_syntax import *
from ..extensions.extended_syntax.spatial_syntax import *
from scipy.spatial.distance import euclidean
from datetime import date
import numpy as np

dataset = ds.DataSet()
adapter = EmployeeVectorAdapter()
dataset.add_adapter(adapter)
dataset.add_adapter(SpatialAdapter())

def test_SimSelectionSyntax():
  # test parsing
  ast = parse_statement('select [1.1, 1.2]')
  """
  ast should look like:

  project: [1.1, 1.2]
     |
  load: ""
  """
  #LogicalPlanViz.show(ast)
  assert isinstance(ast, ProjectionOp) \
    and isinstance(ast.relation, LoadOp)\
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], Vector)
  ast = parse_statement('simselect [1,2,3,4] to [2,4,6,8]')
  """
  ast should look like:

  project: [1,2,3,4] to [2,4,6,8]
     |
  load: ""
  """
  #LogicalPlanViz.show(ast)
  assert isinstance(ast, ProjectionOp) \
    and isinstance(ast.relation, LoadOp)\
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], ToOp)
  ast = parse_statement('select [1,2,3,4] to [2,4,6,8]')
  """
  ast should look like:

  project: [1,2,3,4] to [2,4,6,8]
     |
  load: ""
  """
  #LogicalPlanViz.show(ast)
  assert isinstance(ast, ProjectionOp) \
    and isinstance(ast.relation, LoadOp)\
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], ToOp)
  ast = parse_statement('simselect vector to [1, 2, 3, 4] from employees_with_vectors')
  """
  ast should look like:

  project: vector to [1, 2, 3, 4]
     |
  load: "employees_with_vectors"
  """
  assert isinstance(ast, ProjectionOp) and len(ast.exprs) == 1 and isinstance(ast.exprs[0], ToOp)\
    and isinstance(ast.relation, LoadOp) and ast.relation.name == 'employees_with_vectors'
    
  ast = parse_statement('select employee_id from employees_with_vectors where vector to [1, 2, 3, 4] < 10')
  """
  ast should look like:

  project: employee_id
     |
   simselect: vector to [1, 2, 3, 4] < 10
     |
  load: "employees_with_vectors"
  """
  assert isinstance(ast, ProjectionOp) \
    and isinstance(ast.relation, SimSelectionOp)\
    and isinstance(ast.relation.bool_op, LtOp)\
    and isinstance(ast.relation.bool_op.lhs, ToOp)\
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], Var)
  ast = parse_statement('simselect employee_id from employees_with_vectors where vector to [1, 2, 3, 4] < 10')
  """
  ast should look like:

  project: employee_id
     |
   simselect: vector to [1, 2, 3, 4] < 10
     |
  load: "employees_with_vectors"
  """
  assert isinstance(ast, ProjectionOp) \
    and isinstance(ast.relation, SimSelectionOp)\
    and isinstance(ast.relation.bool_op, LtOp)\
    and isinstance(ast.relation.bool_op.lhs, ToOp)\
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], Var)

  try:
    # should throws an ExtendedSyntaxError since 'to' keyword not found
    ast = parse_statement('simselect vector from employees_with_vectors where true')
    assert False
  except Exception as e:
    assert isinstance(e, ExtendedSyntaxError)
  
  try:
    # should throws an ExtendedSyntaxError since 'to' keyword not found
    ast = parse_statement('simselect vector from employees_with_vectors')
    assert False
  except Exception as e:
    assert isinstance(e, ExtendedSyntaxError)
  

  # test execution
  sql = 'select [1.1, 1.2]'
  exe_res = [row[0] for row in dataset.query(sql).execute()]
  assert len(exe_res) == 1 and exe_res[0].tolist() == [1.1, 1.2]
  sql = 'select [1,2,3,4] to [2,4,6,8]'
  exe_res = [row[0] for row in dataset.query(sql).execute()]
  assert len(exe_res) == 1 and exe_res[0] == euclidean(np.array([1,2,3,4]), np.array([2,4,6,8]))
  sql = 'simselect [1,2,3,4] to [2,4,6,8]'
  exe_res = [row[0] for row in dataset.query(sql).execute()]
  assert len(exe_res) == 1 and exe_res[0] == euclidean(np.array([1,2,3,4]), np.array([2,4,6,8]))
  sql = 'select vector to [1, 2, 3, 4] from employees_with_vectors'
  exe_res = [row[0] for row in dataset.query(sql).execute()]
  src_rel = adapter.get_relation('employees_with_vectors')
  internal_df = src_rel.storage()
  assert len(exe_res) == src_rel.size() 
  df_truth = internal_df.copy(deep=True)
  df_truth['dist'] = df_truth['vector'].apply(lambda v: euclidean(v, np.array([1, 2, 3, 4])))
  assert exe_res == df_truth['dist'].values.tolist()
  sql = 'select employee_id from employees_with_vectors where vector to [1, 2, 3, 4] < 10'
  exe_res = [row[0] for row in dataset.query(sql).execute()]
  assert len(exe_res) == 2
  assert exe_res == [1234, 4567]
  

def test_ExtendedSyntax_mixture():
  
  def exec_sql(sql, truth_res):
    row_idx = 0
    for row in Query(dataset, parse_statement(sql)).execute():
      assert row_idx < len(truth_res)
      for col_idx, col in enumerate(row):
        if isinstance(col, np.ndarray):
          assert isinstance(truth_res[row_idx][col_idx], np.ndarray) \
            and np.array_equal(col, truth_res[row_idx][col_idx]) 
        else:
          assert col == truth_res[row_idx][col_idx]
      row_idx += 1

  # test parsing errors
  sql_01 = """
    select employees_with_vectors.employee_id > {#0, 0#, ab}
    """
  try:
    # should raise an error since the sql is invalid by any syntax
    ast = parse_statement(sql_01)
    assert False
  except SQLSyntaxError as e:
    # 'ab' is invalid for radius claim in a circle
    assert "invalid symbol" in str(e)

  sql_02 = """
    select employees_with_vectors.employee_id > {#0, 0#, 3
    """
  try:
    # should raise an error since the sql is invalid by any syntax
    ast = parse_statement(sql_02)
    assert False
  except SQLSyntaxError as e:
    # "{#0, 0#, 3" is not closed by '}'
    assert "missing closing '}'" == str(e)

  # test single syntax parsing and executing
  sql_11 = """
    select employees_with_vectors.employee_id
    from 
      (simselect * from employees_with_vectors where employees_with_vectors.vector to [1,2,3,4] < 10)
    where employees_with_vectors.employee_id > 1500
    """
  ast = parse_statement(sql_11)
  assert isinstance(ast, ProjectionOp) \
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], Var) \
    and ast.exprs[0].path == 'employees_with_vectors.employee_id'\
    and isinstance(ast.relation, SelectionOp) and isinstance(ast.relation.bool_op, GtOp) \
    and isinstance(ast.relation.relation, SimSelectionOp) \
    and isinstance(ast.relation.relation.bool_op, LtOp) and isinstance(ast.relation.relation.bool_op.lhs, ToOp) \
    and isinstance(ast.relation.relation.relation, LoadOp)
  exec_sql(sql_11, [(4567, )])

  sql_12 = """
    spatialselect pid from points where point inside {#0,0#, 3}
    """
  ast = parse_statement(sql_12)
  assert isinstance(ast, ProjectionOp) \
    and len(ast.exprs) == 1 and isinstance(ast.exprs[0], Var) \
    and ast.exprs[0].path == 'pid'\
    and isinstance(ast.relation, SpatialSelectionOp) and isinstance(ast.relation.bool_op, InsideOp) \
    and isinstance(ast.relation.relation, LoadOp) and ast.relation.relation.name == 'points' 
  exec_sql(sql_12, [(1234, ), (4567, )])

  # test multiple syntax mixture parsing and executing
  sql_21 = """
    simselect * from employees_with_vectors where employees_with_vectors.vector to [1,2,3,4] < 10 OR #1,2# inside {#0,0#, 10}
    """
  ast = parse_statement(sql_21)
  assert isinstance(ast, SimSelectionOp) and isinstance(ast.bool_op, Or) \
    and isinstance(ast.bool_op.lhs, LtOp) and isinstance(ast.bool_op.lhs.lhs, ToOp) and isinstance(ast.bool_op.rhs, InsideOp) \
    and isinstance(ast.relation, LoadOp) and ast.relation.name == 'employees_with_vectors'
  exec_sql(sql_21, [
    (1234, 
      "Tom Tompson", 
      date(2009,1,17), 
      -1, 
      (), 
      np.array([1,2,3,4])
    ), 
    (
      4567, 
      "Sally Sanders",
      date(2010,2,24),
      1234,
      (), 
      np.array([4,5,6,7])
    ), 
    (
      8901, 
      "Mark Markty",
      date(2010,3,1),
      1234,
      ('sales', 'marketing'), 
      np.array([8,9,0,1])
    )
  ])

  sql_22 = """
    select employees_with_vectors.employee_id, points.pid
    from 
     (simselect * from employees_with_vectors where employees_with_vectors.vector to [1,2,3,4] < 10)
     JOIN points ON points.pid = employees_with_vectors.employee_id
    where employees_with_vectors.employee_id > 1500 AND points.point inside {#0,0#, 3}
    """
  ast = parse_statement(sql_22)
  assert isinstance(ast, ProjectionOp) \
    and len(ast.exprs) == 2 and ast.exprs[0].path == 'employees_with_vectors.employee_id' and ast.exprs[1].path == 'points.pid' \
    and isinstance(ast.relation, SpatialSelectionOp) and isinstance(ast.relation.bool_op, And) \
    and isinstance(ast.relation.bool_op.lhs, GtOp) and isinstance(ast.relation.bool_op.rhs, InsideOp) \
    and isinstance(ast.relation.relation, JoinOp) and isinstance(ast.relation.relation.bool_op, EqOp) \
    and isinstance(ast.relation.relation.left, SimSelectionOp) and isinstance(ast.relation.relation.right, LoadOp) \
    and isinstance(ast.relation.relation.left.bool_op, LtOp) and isinstance(ast.relation.relation.left.bool_op.lhs, ToOp) \
    and isinstance(ast.relation.relation.left.relation, LoadOp) \
    and ast.relation.relation.left.relation.name == "employees_with_vectors" and ast.relation.relation.right.name == "points" 
  exec_sql(sql_22, [(4567, 4567)])
  