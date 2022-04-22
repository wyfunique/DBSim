import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dbsim import dataset as ds
from dbsim.tests.fixtures.demo_adapter import DemoAdapter
from dbsim.query_parser import parse_statement
from dbsim.query import Query
from dbsim.planners import rules
from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner

dataset = ds.DataSet()
dataset.add_adapter(DemoAdapter())

planner = HeuristicPlanner(max_limit = float('Inf'))
planner.addRule(rules.FilterMergeRule())
planner.addRule(rules.FilterPushDownRule())
planner.addRule(rules.Selection_SimSelection_Swap_Rule())

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
# without query optimization
print('------------------')
for row in Query(dataset, parse_statement(standard_sql)):
  print(row)
# with query optimization
print('------------------')
for row in Query(dataset, parse_statement(standard_sql), optimizer=planner):
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
# without query optimization
print('------------------')
for row in Query(dataset, parse_statement(extended_syntax_sql)):
  print(row)
# with query optimization
print('------------------')
for row in Query(dataset, parse_statement(extended_syntax_sql), optimizer=planner):
  print(row)
