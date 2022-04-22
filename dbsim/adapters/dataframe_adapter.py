from dbsim import Table
from pandas import DataFrame
from . import Adapter

class DataFrameAdapter(Adapter):
  """
  An adapter for working with Pandas DataFrame
  """
  def __init__(self, **tables):
    """

    Examples:
    Dictionary(
      users=dict(
          schema=[], 
          dataframe=pd.DataFrame(...)
      )
      other=dict(
        schema=[],
        dataframe=pd.DataFrame(...)
      )
    )

    """
    self._tables = {}

    for name, table in tables.items():
      if isinstance(table, dict):
        schema = table['schema']
        df = table['dataframe']
      else:
        raise RuntimeError("Invalid table setup for '{}', please input the table using Python dict and specify its schema".format(name))
        
      self._tables[name] = DataFrameTable(
        self,
        name, 
        schema=schema, 
        df=df
      )


  @property
  def relations(self):
    return [
      (name, table.schema)
      for name, table in self._tables.items()
    ]


  def has(self, relation):
    return relation in self._tables

  def schema(self, relation):
    return self._tables[relation].schema

  def get_relation(self, name):
    return self._tables.get(name)

  def table_scan(self, name, ctx):
    return self._tables[name]



class DataFrameTable(Table):
  def __init__(self, adapter, name, schema, df):
    super(self.__class__, self).__init__(adapter, name, schema)
    self.key_index = [
      (f.name, () if f.mode == 'REPEATED' else None)
      for f in self.schema.fields
    ]
    self._df = df

  def __iter__(self):
    key_index = self.key_index

    # This will return a generator, which acts the same as an iterator
    return (
      tuple(row.get(key, default=default) for key, default in key_index)
      for i, row in self._df.iterrows()
    )

  def df(self):
    return self._df
  
  def storage(self):
    return self.df()

  def size(self):
    return len(self._df)