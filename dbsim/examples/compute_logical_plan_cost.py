import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dbsim import dataset as ds
from dbsim.tests.fixtures.demo_adapter import DemoAdapter
from dbsim.query_parser import parse_statement
from dbsim.query import Query
from dbsim.planners import rules
from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
from dbsim.planners.cost.logical_cost import LogicalCost


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
query = Query(dataset, parse_statement(sql))
plan = query.getPlan()
best_plan = planner.findBestPlan(plan)

# refineCostFactors will compute the up-to-date cost factor 
#   of each plan node based on its descendants. 
# This method must be called before getCost, 
#   otherwise the cost computation will be less accurate.
# See Github Wiki of this repo for more details on logical cost computation. 
LogicalCost.refineCostFactors(plan, False)
LogicalCost.refineCostFactors(best_plan, False)
# After refineCostFactors, call getCost to return the cost of the input plan.
print("Cost (initial plan): ", LogicalCost.getCost(plan, dataset).toNumeric())
print("Cost (best plan): ", LogicalCost.getCost(best_plan, dataset).toNumeric())

print("Results:\n----------------")
for row in Query(dataset, best_plan, False):
  print(row)
