from dbsim import dataset as ds
from dbsim.tests.fixtures.demo_adapter import DemoAdapter
from dbsim.query_parser import parse_statement
from dbsim.query import Query
from dbsim.utils.visualizer import LogicalPlanViz 
from dbsim.planners import rules
from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
from dbsim.planners.cost.logical_cost import LogicalCost


dataset = ds.DataSet()
dataset.add_adapter(DemoAdapter())

sql = """
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
query = Query(dataset, parse_statement(sql))
plan = query.getPlan()
#LogicalPlanViz.show(plan)

"""
  The plan should look like:
    projection: musical.title, musical.year
          |
    selection: musical.year > 1960
          |
    simselection: animation.embedding to [1,2,3,4] < 10
          |
    selection: animation.mid = musical.mid
          |              
        join: true
    /                 \   
Relation: animation  Relation: musical
"""
planner = HeuristicPlanner(max_limit = float('Inf'))
planner.addRule(rules.FilterMergeRule())
planner.addRule(rules.FilterPushDownRule())
planner.addRule(rules.Selection_SimSelection_Swap_Rule())
best_plan = planner.findBestPlan(plan)
"""
  The best_plan should look like:
    projection: musical.title, musical.year
                |
    simselection: animation.embedding to [1,2,3,4] < 10
                |
    selection: animation.mid = musical.mid
                |              
            join: true
    /                         \   
Relation: animation       selection:
                          musical.year > 1960
                                |
                          Relation: musical

    
"""
#LogicalPlanViz.show(best_plan)

LogicalCost.refineCostFactors(plan, False)
LogicalCost.refineCostFactors(best_plan, False)

#LogicalPlanViz.show(plan)

print("Results:\n----------------")
for row in Query(dataset, best_plan, False):
  print(row)
print("Cost (init plan): ", LogicalCost.getCost(plan, dataset).toNumeric())
print("Cost (best plan): ", LogicalCost.getCost(best_plan, dataset).toNumeric())
print("Results by integrated optimization:\n----------------")
for row in Query(dataset, parse_statement(sql), optimizer=planner):
  print(row)

assert Query(dataset, best_plan, False).get_pretty_results() \
  == Query(dataset, parse_statement(sql), optimizer=planner).get_pretty_results()