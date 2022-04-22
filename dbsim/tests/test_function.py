from .. import dataset as ds
from .fixtures.employee_adapter import EmployeeAdapter

dataset = ds.DataSet()
  
def test_init_functions():
  assert len(dataset.udfs) > 0

def test_call_math_functions():
  cos = dataset.get_function('cos')
  sin = dataset.get_function('sin')
  assert cos(0) == 1 and sin(0) == 0

