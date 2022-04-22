from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeAdapter, EmployeeDataFrameAdapter

truth_dataset = ds.DataSet()
df_dataset = ds.DataSet()
truth_adapter = EmployeeAdapter()
df_adapter = EmployeeDataFrameAdapter()
truth_dataset.add_adapter(truth_adapter)
df_dataset.add_adapter(df_adapter)
  
def test_df_adapter_count():
  col = 'employee_id'
  truth_query = truth_dataset.query('select count({col}) from employees'.format(col=col))
  truth_res = truth_query.get_pretty_results()
  df_query = df_dataset.query('select count({col}) from employees'.format(col=col))
  df_res = df_query.get_pretty_results()
  assert truth_res[0][0] == truth_adapter.get_relation('employees').size() and truth_res == df_res

def test_df_adapter_min_max():
  col = 'employee_id'
  truth_query_min = truth_dataset.query('select min({col}) from employees'.format(col=col))
  truth_query_max = truth_dataset.query('select max({col}) from employees'.format(col=col))
  truth_res_min = truth_query_min.get_pretty_results()
  truth_res_max = truth_query_max.get_pretty_results()
  df_query_min = df_dataset.query('select min({col}) from employees'.format(col=col))
  df_query_max = df_dataset.query('select max({col}) from employees'.format(col=col))
  df_res_min = df_query_min.get_pretty_results()
  df_res_max = df_query_max.get_pretty_results()
  
  true_min = float('Inf')
  true_max = float('-Inf')
  for row in truth_adapter.get_relation('employees')._rows:
    true_min = min(true_min, row[col])
    true_max = max(true_max, row[col])
  assert truth_res_min[0][0] == true_min and truth_res_max[0][0] == true_max and truth_res_max == df_res_max and truth_res_min == df_res_min

def test_df_adapter_concat():
  col = 'full_name'
  truth_query = truth_dataset.query('select concat({col}) from employees'.format(col=col))
  truth_res = truth_query.get_pretty_results()
  df_query = df_dataset.query('select concat({col}) from employees'.format(col=col))
  df_res = df_query.get_pretty_results()
  
  truth = ""
  for row in truth_adapter.get_relation('employees')._rows:
    truth += row[col]
  assert truth_res[0][0] == truth and truth_res == df_res

def test_df_adapter_sum():
  col = 'employee_id'
  truth_query = truth_dataset.query('select sum({col}) from employees'.format(col=col))
  truth_res = truth_query.get_pretty_results()
  df_query = df_dataset.query('select sum({col}) from employees'.format(col=col))
  df_res = df_query.get_pretty_results()

  truth = 0
  for row in truth_adapter.get_relation('employees')._rows:
    truth += row[col]
  assert truth_res[0][0] == truth and truth_res == df_res

