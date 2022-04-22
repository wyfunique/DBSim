from dbsim.adapters.dict_adapter import DictAdapter 
from dbsim.adapters.dataframe_adapter import DataFrameAdapter 
from datetime import date
from pandas import DataFrame
from ...field import FieldType
import numpy as np

employee_records = [
  dict(
    employee_id=1234, 
    full_name="Tom Tompson", 
    employment_date=date(2009,1,17)
  ),
  dict(
    employee_id=4567, 
    full_name="Sally Sanders",
    employment_date=date(2010,2,24),
    manager_id = 1234
  ),
  dict(
    employee_id=8901, 
    full_name="Mark Markty",
    employment_date=date(2010,3,1),
    manager_id = 1234,
    roles = ('sales', 'marketing')
  )
]

class EmployeeAdapter(DictAdapter):
  def __init__(self):
    super(self.__class__, self).__init__(
      employees = dict(
        schema = dict(
          fields=[
            dict(name="employee_id", type=FieldType.INTEGER),
            dict(name="full_name", type=FieldType.STRING),
            dict(name="employment_date", type=FieldType.DATE),
            dict(name="manager_id", type=FieldType.INTEGER),
            dict(name="roles", type=FieldType.STRING, mode="REPEATED")
          ]
        ),
        rows = employee_records
      )
    )

employee_records_dataframe = DataFrame([
    (
      1234, 
      "Tom Tompson", 
      date(2009,1,17), 
      -1, 
      ()
    ),
    (
      4567, 
      "Sally Sanders",
      date(2010,2,24),
      1234,
      ()
    ),
    (
      8901, 
      "Mark Markty",
      date(2010,3,1),
      1234,
      ('sales', 'marketing')
    )
], columns = ["employee_id", "full_name", "employment_date", "manager_id", "roles"])

employee_records_table2_dataframe = DataFrame([
    (
      1234, 
      "Tom Tompson", 
      date(2009,1,17), 
      -1, 
      ()
    ),
    (
      4567, 
      "Sally Sanders",
      date(2010,2,24),
      1234,
      ()
    ),
    (
      9999, 
      "Mark Markty Clever",
      date(2010,3,1),
      1234,
      ('sales', 'marketing')
    )
], columns = ["employee_id", "full_name", "employment_date", "manager_id", "roles"])

class EmployeeDataFrameAdapter(DataFrameAdapter):
  def __init__(self):
    super(self.__class__, self).__init__(
      employees = dict(
        schema = dict(
          fields=[
            dict(name="employee_id", type=FieldType.INTEGER),
            dict(name="full_name", type=FieldType.STRING),
            dict(name="employment_date", type=FieldType.DATE),
            dict(name="manager_id", type=FieldType.INTEGER),
            dict(name="roles", type=FieldType.STRING, mode="REPEATED")
          ]
        ),
        dataframe = employee_records_dataframe
      ), 
      employees_2 = dict(
        schema = dict(
          fields=[
            dict(name="employee_id", type=FieldType.INTEGER),
            dict(name="full_name", type=FieldType.STRING),
            dict(name="employment_date", type=FieldType.DATE),
            dict(name="manager_id", type=FieldType.INTEGER),
            dict(name="roles", type=FieldType.STRING, mode="REPEATED")
          ]
        ),
        dataframe = employee_records_table2_dataframe
      )
    )


employee_vectors_dataframe = DataFrame([
    (
      1234, 
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
], columns = ["employee_id", "full_name", "employment_date", "manager_id", "roles", "vector"])

class EmployeeVectorAdapter(DataFrameAdapter):
  def __init__(self):
    super(self.__class__, self).__init__(
      employees_with_vectors = dict(
        schema = dict(
          fields=[
            dict(name="employee_id", type=FieldType.INTEGER),
            dict(name="full_name", type=FieldType.STRING),
            dict(name="employment_date", type=FieldType.DATE),
            dict(name="manager_id", type=FieldType.INTEGER),
            dict(name="roles", type=FieldType.STRING, mode="REPEATED"), 
            dict(name="vector", type=FieldType.VECTOR)
          ]
        ),
        dataframe = employee_vectors_dataframe
      )
    )

