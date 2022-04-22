from .filter_merge_rule import * 
from .filter_push_down_rule import * 
from .selection_simselection_swap_rule import *

ruleName2ruleClass = {
  name: globals()[name] for name in globals() if name.endswith('Rule') and name != "Rule"
}

def getAllRulesNames():
  return ruleName2ruleClass.keys()