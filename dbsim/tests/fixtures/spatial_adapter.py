from dbsim.adapters.dataframe_adapter import DataFrameAdapter 
from pandas import DataFrame
from ...field import FieldType
from ...extensions.extended_syntax.spatial_syntax import Point, Circle


points = DataFrame([
  (
    1234, 
    Point(0, 0)
  ),
  (
    4567, 
    Point(1, 2)
  ),
  (
    8901, 
    Point(10, 3)
  )
], columns = ["pid", "point"])

circles = DataFrame([
  (
    1001, 
    Circle(Point(3, 4), 5)
  ),
  (
    1002, 
    Circle(Point(1, 1), 10)
  ),
  (
    1003, 
    Circle(Point(10, 10), 100)
  )
], columns = ["cid", "circle"])

class SpatialAdapter(DataFrameAdapter):
  def __init__(self):
    super(self.__class__, self).__init__(
      points = dict(
        schema = dict(
          fields=[
            dict(name="pid", type=FieldType.INTEGER),
            dict(name="point", type=FieldType.POINT),
          ]
        ),
        dataframe = points
      ), 
      circles = dict(
        schema = dict(
          fields=[
            dict(name="cid", type=FieldType.INTEGER),
            dict(name="circle", type=FieldType.CIRCLE),
          ]
        ),
        dataframe = circles
      )
    )


