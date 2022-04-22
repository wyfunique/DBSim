from collections import OrderedDict
from .extended_syntax import ExtendedSyntax
from .registry_utils import *
from .sim_select_syntax import *
from .spatial_syntax import *
from ...utils.logger import Logger

logger = Logger.general_logger

"""
The registry is a dict(name -> RegEntry) 
  where the name is the string to identify the following entry which should be unique.

Note that when there are multiple entries in the registry, 
  for each distinct SQLClause, there may be multiple trigger functions to be True at the same time.
  Only the parser function corresponding to the first True trigger function will be called to parse the clause, 
  while others will be ignored. 
"""
registry: Registry = OrderedDict({
  "simselect": RegEntry( 
                  syntax=SimSelectionSyntax, 
                  clause_parsers=OrderedDict({
                    SQLClause.SELECT: ("trigger_simselect", "parse_simselect"), 
                    SQLClause.WHERE: ("trigger_simselect_where", "parse_simselect_where")
                  }), 
                  entry_points=[
                    SimSelectionSyntax.addExtendedSymbolsAndKeywords,
                    SimSelectionSyntax.addExtendedPredicateParsers,
                    SimSelectionSyntax.addExtendedDataTypes,
                    SimSelectionSyntax.addExtendedPredicateOps,
                    SimSelectionSyntax.addExtendedRelationOps
                  ]
                ),
  "spitial": RegEntry( 
                  syntax=SpatialSyntax, 
                  clause_parsers=OrderedDict({
                    SQLClause.SELECT: ("trigger_spatialselect", "parse_spatialselect"), 
                    SQLClause.WHERE: ("trigger_spatialselect_where", "parse_spatialselect_where")
                  }), 
                  entry_points=[
                    SpatialSyntax.addExtendedSymbolsAndKeywords,
                    SpatialSyntax.addExtendedPredicateParsers,
                    SpatialSyntax.addExtendedDataTypes,
                    SpatialSyntax.addExtendedPredicateOps,
                    SpatialSyntax.addExtendedRelationOps
                  ]
                ),
})


isRegInitilized = False

def getRegistry() -> Registry:
  return registry

def validateRegistry():
  ERROR_IF_NOT_INSTANCE_OF(
    registry, OrderedDict, 
    "The registry must be an OrderedDict(should not be {}).".format(type(registry)), 
    RegistryError
  )
  for syntax_name, reg_entry in registry.items():
    ERROR_IF_NOT_INSTANCE_OF(
      reg_entry.clause_parsers, OrderedDict, 
      "RegEntry.clause_parsers must be an OrderedDict(should not be {}).".format(type(reg_entry.clause_parsers)), 
      RegistryError
    )

def initRegistry():
  global isRegInitilized
  if isRegInitilized:
    return
  validateRegistry()
  for syntax in registry:
    # calls the functions mounted at entry points 
    #   to inject extended values and methods
    reg_entry = registry[syntax]
    for func in reg_entry.entry_points:
      func()
  isRegInitilized = True

initRegistry()
