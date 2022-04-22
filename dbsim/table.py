from .schema import Schema

class Table(object):
  def __init__(self, adapter, name, schema):
    """

    Initialize a table with a name or a schema.

    Args:
      name (str):  The name of the table.
      schema (Schema | dict): Schema for the table specified as a dict
      or instance of the Schema class.

    """
    self.adapter = adapter
    self.name = name

    if isinstance(schema, dict):
      args = schema.copy()
      if 'name' not in args:
        args['name'] = name
      schema = Schema(**args)
    self.schema = schema

  @property
  def fields(self):
    return self.schema.fields


  def __iter__(self):
    """
    Returns an iterator of tuples. 
    """
    return iter([])