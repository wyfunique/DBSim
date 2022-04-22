from . import math
from . import string
from . import relational


def register_on(dataset):
  math.register_on(dataset)
  string.register_on(dataset)
  relational.register_on(dataset)
