import tornado.ioloop
import tornado.web
import json
import os
import time
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from dbsim import dataset as ds
from dbsim.adapters.adapter_factory import AdapterFactory
from dbsim.query_parser import parse_statement
from dbsim.query import Query
from dbsim.ast import *
from dbsim.utils.visualizer import LogicalPlanViz 
from dbsim.utils.exceptions import *
from dbsim.planners import rules
from dbsim.planners import planner
from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
from dbsim.compilers import local
from dbsim.planners.cost.logical_cost import LogicalCost
from dbsim.extensions.extended_syntax import registry 

SUCCESS = 0
FAILURE = -1

ds_folder = "dbsim/gui/ds"

dataset = ds.DataSet()
planner = HeuristicPlanner(max_limit = float('Inf'))

def refreshDatasets():
  dataset.reset()
  for ds_filename in os.listdir(ds_folder):
    ds_filepath = os.path.join(ds_folder, ds_filename)
    ds_name = ds_filename.split('.')[0]
    dataset.add_adapter(AdapterFactory.fromFile(ds_filepath, ds_name))

def initialize():
  registry.initRegistry()
  refreshDatasets()

def executeQuery(sql):
  query = Query(dataset, parse_statement(sql))
  plan = query.getPlan()
  best_plan = planner.findBestPlan(plan)

  LogicalPlanViz.show(
    plan, img_save_dir=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static', 'img'), 
    img_name='plan'  
  )
  LogicalPlanViz.show(
    best_plan, img_save_dir=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static', 'img'), 
    img_name='best_plan'  
  )

  LogicalCost.refineCostFactors(plan, False)
  LogicalCost.refineCostFactors(best_plan, False)

  print("Results:\n----------------")
  for row in Query(dataset, best_plan, False):
    print(row)
  plan_cost = LogicalCost.getCost(plan, dataset).toNumeric()
  best_cost = LogicalCost.getCost(best_plan, dataset).toNumeric()
  print("Cost (init plan): ", plan_cost)
  print("Cost (best plan): ", best_cost)
  start = time.time()
  results, headers = query.get_pretty_results_with_headers()
  end = time.time()
  return [list(row) for row in results], headers, plan_cost, best_cost, (end-start)

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    self.render("../index.html")

class QueryHandler(tornado.web.RequestHandler):
  def post(self):
    data = json.loads(self.request.body)
    query = data['query']
    results, headers, plan_cost, best_cost, exec_time = executeQuery(query)
    response = json.dumps({
      'results': results, 'headers': headers,  
      'plan_cost': float("{:.2f}".format(plan_cost)), 'best_cost': float("{:.2f}".format(best_cost)), 
      'exec_time': float("{:.4f}".format(exec_time))
    })
    self.write(response)

class RulesHandler(tornado.web.RequestHandler):
  def get(self):
    all_rules = rules.getAllRulesNames()
    applied_rules = planner.getRulesNames()
    response = json.dumps({
      'applied_rules': applied_rules, 
      'non_applied_rules': list(set(all_rules) - set(applied_rules))
    })
    self.write(response)

  def post(self):
    data = json.loads(self.request.body)
    updated_applied_rules_list = data['applied_rules']
    print(updated_applied_rules_list)
    if updated_applied_rules_list == planner.getRulesNames():
      response = json.dumps({
        'status': FAILURE, 
        'msg': "Rules did not change."
      })
    else:
      planner.clearRules()
      for rule_name in updated_applied_rules_list:
        planner.addRule(rules.ruleName2ruleClass[rule_name]())
      applied_rules = planner.getRulesNames()
      response = json.dumps({
        'status': SUCCESS, 
        'applied_rules': applied_rules
      })
    self.write(response)

def getDatasetsNames():
  return [ 
          filename_with_ext.split('.')[0] for filename_with_ext in os.listdir(ds_folder)
        ]

class DatasetsHandler(tornado.web.RequestHandler):
  def get(self):
    response = json.dumps({
        'status': SUCCESS, 
        'datasets': getDatasetsNames()
    })
    self.write(response)

  def post(self):
    uploaded_ds_file = self.request.files['file'][0]
    uploaded_ds_filename_with_ext = uploaded_ds_file['filename']
    if len(uploaded_ds_filename_with_ext.split('.')) > 2:
      response = json.dumps({
        'status': FAILURE, 
        'msg': "Invalid dataset filename, at most one dot symbol can be included in it."
      })
      self.write(response)
      return

    uploaded_ds_filename = uploaded_ds_filename_with_ext.split('.')[0]
    print(uploaded_ds_filename)
    
    if uploaded_ds_filename in getDatasetsNames():
      response = json.dumps({
        'status': FAILURE, 
        'msg': "Dataset '{}' already exists. Please remove it first.".format(uploaded_ds_filename)
      })
    else:
      filepath = os.path.join(ds_folder, uploaded_ds_filename_with_ext)
      with open(filepath, 'wb+') as saved_file:
        saved_file.write(uploaded_ds_file['body'])
      dataset.add_adapter(AdapterFactory.fromFile(filepath, ds_name = uploaded_ds_filename))
      print("updated dataset after adding: {}".format(dataset.relations))
      response = json.dumps({
        'status': SUCCESS, 
        'datasets': getDatasetsNames()
      })
    self.write(response)
  
  def delete(self):
    ds_name = self.get_argument("name", None, True)
    print("delete: " + ds_name)
    try:
      ds_file_index = getDatasetsNames().index(ds_name)
      dataset.remove_adapter(dataset.adapter_for(ds_name))
      os.remove(os.path.join(ds_folder, os.listdir(ds_folder)[ds_file_index]))
      print("updated dataset after removal: {}".format(dataset.relations))
      response = json.dumps({
        'status': SUCCESS, 
        'datasets': getDatasetsNames()
      })
    except Exception as e:
      if isinstance(e, ValueError):
        # ds_name not found in the existing dataset files
        response = json.dumps({
          'status': FAILURE, 
          'msg': "Dataset {} not exists.".format(ds_name)
        })
      else:
        response = json.dumps({
          'status': FAILURE, 
          'msg': "500 server internal error"
        })
        self.write(response)
        raise e
    self.write(response)
  
def make_app():
  settings = {
    "static_path": os.path.join(os.path.dirname(os.path.dirname(__file__)), "static"),
  }
  return tornado.web.Application([
      (r"/", MainHandler),
      (r"/query", QueryHandler),
      (r"/rules", RulesHandler), 
      (r"/ds", DatasetsHandler)
  ], **settings)

if __name__ == "__main__":
  initialize()
  app = make_app()
  app.listen(8888)
  tornado.ioloop.IOLoop.current().start()