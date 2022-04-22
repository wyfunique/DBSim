from dbsim.adapters.dataframe_adapter import DataFrameAdapter 
from datetime import date
from pandas import DataFrame
from ...field import FieldType
import numpy as np

"""
The demo scenario and datasets are same as 
https://github.com/milvus-io/bootcamp/tree/master/solutions/recommender_system.

Here we use some small samples of the whole datasets for a very simple end-to-end test. 
"""

"""Users of a online movie website"""
demo_dataset_users = DataFrame([
    (
      1234, 
      "Tom", 
      date(2009,1,17), 
      np.array([1,2,3,4])
    ),
    (
      4567, 
      "Sally",
      date(2010,2,24),
      np.array([4,5,6,7])
    ),
    (
      8901, 
      "Mark",
      date(2010,3,1),
      np.array([8,9,0,1])
    ), 
    (
      9999, 
      "Tony",
      date(2010,3,1),
      np.array([9,9,9,9])
    ), 
], columns = ["uid", "name", "signUpDate", "embedding"])

"""The movies collection of genre 'Animation'"""
demo_dataset_movies_animation = DataFrame([
    (
      1234, 
      "Toy Story", 
      1995, 
      np.array([1,2,3,4])
    ),
    (
      4567, 
      "Balto",
      1995,
      np.array([4,5,6,7])
    ), 
    (
      6789, 
      "Swan Princess", 
      1994, 
      np.array([6,7,8,9])
    ),(
      1011, 
      "Aladdin", 
      1992, 
      np.array([1,0,1,1])
    ),(
      1235, 
      "Snow White and the Seven Dwarfs", 
      1937, 
      np.array([1,2,3,5])
    ), 
    (
      1236, 
      "Beauty and the Beast", 
      1991, 
      np.array([1,2,3,6])
    )
], columns = ["mid", "title", "year", "embedding"])

"""The movies collection of genre 'Musical'"""
demo_dataset_movies_musical = DataFrame([
    (
      1235, 
      "Snow White and the Seven Dwarfs", 
      1937, 
      np.array([1,2,3,5])
    ), 
    (
      1236, 
      "Beauty and the Beast", 
      1991, 
      np.array([1,2,3,6])
    ), 
    (
      1011, 
      "Aladdin", 
      1992, 
      np.array([1,0,1,1])
    ), 
    (
      9800, 
      "Singin' in the Rain", 
      1952, 
      np.array([9,8,0,0])
    ), 
    (
      9858, 
      "American in Paris", 
      1951, 
      np.array([9,8,5,8])
    ), 
    
], columns = ["mid", "title", "year", "embedding"])

class DemoAdapter(DataFrameAdapter):
  def __init__(self):
    super(self.__class__, self).__init__(
      users = dict(
        schema = dict(
          fields=[
            dict(name="uid", type=FieldType.INTEGER),
            dict(name="name", type=FieldType.STRING),
            dict(name="signUpDate", type=FieldType.DATE),
            dict(name="embedding", type=FieldType.VECTOR)
          ]
        ),
        dataframe = demo_dataset_users
      ), 
      animation = dict(
        schema = dict(
          fields=[
            dict(name="mid", type=FieldType.INTEGER),
            dict(name="title", type=FieldType.STRING),
            dict(name="year", type=FieldType.INTEGER),
            dict(name="embedding", type=FieldType.VECTOR)
          ]
        ),
        dataframe = demo_dataset_movies_animation
      ), 
      musical = dict(
        schema = dict(
          fields=[
            dict(name="mid", type=FieldType.INTEGER),
            dict(name="title", type=FieldType.STRING),
            dict(name="year", type=FieldType.INTEGER),
            dict(name="embedding", type=FieldType.VECTOR)
          ]
        ),
        dataframe = demo_dataset_movies_musical
      )
    )
