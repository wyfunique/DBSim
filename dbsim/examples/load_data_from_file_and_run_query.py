import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dbsim import dataset as ds
from dbsim.adapters.adapter_factory import AdapterFactory
from dbsim.query_parser import parse_statement
from dbsim.query import Query

dataset = ds.DataSet()

# load data adapter from csv/tsv file
animation_adapter = AdapterFactory.fromFile(
  os.path.join(os.path.dirname(__file__), "./example_ds/animation.csv"), 
  ds_name="animation"
)
musical_adapter = AdapterFactory.fromFile(
  os.path.join(os.path.dirname(__file__), "./example_ds/musical.csv"), 
  ds_name="musical"
)
# add them into the dataset
dataset.add_adapter(animation_adapter)
dataset.add_adapter(musical_adapter)

standard_sql = """
    SELECT musical.title, musical.year
    FROM 
      (SELECT * 
        FROM 
          (animation JOIN musical ON animation.mid = musical.mid) 
        WHERE 
          animation.mid < 1200
      )
    WHERE musical.year > 1960
"""
print('------------------')
for row in Query(dataset, parse_statement(standard_sql)):
  print(row)

extended_syntax_sql = """
    SELECT musical.title, musical.year
    FROM 
      (SELECT * 
        FROM 
          (SELECT * FROM animation, musical WHERE animation.mid = musical.mid) 
        WHERE 
          animation.embedding to [1,2,3,4] < 10
      )
    WHERE musical.year > 1960
"""
print('------------------')
for row in Query(dataset, parse_statement(extended_syntax_sql)):
  print(row)
