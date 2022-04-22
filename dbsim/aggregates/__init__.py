from dbsim.field import Field, FieldType

"""
Configuration for the aggregate functions.

To add more aggregates, simply append a new Aggregate object into AggConfTable.
"""

class Aggregate(object):
  def __init__(self, name, func_body, returns, initial=None, finalize=None):
    self.name = name
    self.func_body = func_body
    self.returns = returns
    self.initial  = initial
    self.finalize = finalize
    #self.state = None

  def __call__(self, *args):
    return args

AggConfTable = [    
    Aggregate("count", lambda state: state + 1, Field(name="count", type=FieldType.INTEGER), 0, None), 
    Aggregate("min", min, Field(name="min", type=FieldType.INTEGER), float('Inf'), None),  
    Aggregate("max", max, Field(name="max", type=FieldType.INTEGER), float('-Inf'), None),  
    # the Python builtin function 'sum' does not work here, instead, we use lambda function to implement it as an accumulation process.
    Aggregate("sum", lambda state, next: state + next, Field(name="sum", type=FieldType.INTEGER), 0, None),
    Aggregate("concat", lambda state_str, cur_str: state_str + cur_str, Field(name="concat", type=FieldType.STRING), "", None), 
]

def register_on(dataset):
    for agg_conf_entry in AggConfTable:
        dataset.add_aggregate(
            agg_conf_entry.name, 
            agg_conf_entry.func_body,
            agg_conf_entry.returns,
            agg_conf_entry.initial,
            agg_conf_entry.finalize
        )

  