from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeAdapter

dataset = ds.DataSet()
adapter = EmployeeAdapter()
dataset.add_adapter(adapter)
  
def test_aggregate_count():
  col = 'employee_id'
  query = dataset.query('select count({col}) from employees'.format(col=col))
  res = query.get_pretty_results()
  assert res[0][0] == adapter.get_relation('employees').size()

def test_aggregate_min_max():
  col = 'employee_id'
  query_min = dataset.query('select min({col}) from employees'.format(col=col))
  query_max = dataset.query('select max({col}) from employees'.format(col=col))
  res_min = query_min.get_pretty_results()
  res_max = query_max.get_pretty_results()
  true_min = float('Inf')
  true_max = float('-Inf')
  for row in adapter.get_relation('employees')._rows:
    true_min = min(true_min, row[col])
    true_max = max(true_max, row[col])
  assert res_min[0][0] == true_min and res_max[0][0] == true_max

def test_aggregate_concat():
  col = 'full_name'
  query = dataset.query('select concat({col}) from employees'.format(col=col))
  res = query.get_pretty_results()
  truth = ""
  for row in adapter.get_relation('employees')._rows:
    truth += row[col]
  assert res[0][0] == truth

def test_aggregate_sum():
  col = 'employee_id'
  query = dataset.query('select sum({col}) from employees'.format(col=col))
  res = query.get_pretty_results()
  truth = 0
  for row in adapter.get_relation('employees')._rows:
    truth += row[col]
  assert res[0][0] == truth

