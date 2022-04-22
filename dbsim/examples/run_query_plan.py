import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dbsim import dataset as ds
from dbsim.tests.fixtures.demo_adapter import DemoAdapter
from dbsim.query_parser import parse_statement
from dbsim.query import Query
from dbsim.planners import rules
from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
from dbsim.utils.visualizer import LogicalPlanViz 

dataset = ds.DataSet()
dataset.add_adapter(DemoAdapter())

planner = HeuristicPlanner(max_limit = float('Inf'))
planner.addRule(rules.FilterMergeRule())
planner.addRule(rules.FilterPushDownRule())
planner.addRule(rules.Selection_SimSelection_Swap_Rule())

sql = """
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

# manually generate logical plans and visualize them
ast = parse_statement(sql)
plan = Query(dataset, ast).getPlan()
best_plan = planner.findBestPlan(plan)
LogicalPlanViz.show(plan, view=True)
LogicalPlanViz.show(best_plan, view=True)

# execute the generated plans directly.
# resolve_op_schema=False will prevent the constructor of Query 
#   from modifying the input plan.
print('------------------')
for row in Query(dataset, plan, resolve_op_schema=False):
  print(row)
print('------------------')
for row in Query(dataset, best_plan, resolve_op_schema=False):
  print(row)
