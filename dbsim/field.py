import typing
from enum import Enum

class FieldType(Enum):
  INTEGER = 'INTEGER'
  FLOAT = 'FLOAT'
  STRING = 'STRING'
  BOOLEAN = 'BOOLEAN'
  DATE = 'DATE'
  DATETIME = 'DATETIME'
  TIME = 'TIME'
  RECORD = 'RECORD'
  NULL = 'NULL'

def addDataTypes(new_data_types: typing.Dict[str, str]):
  global FieldType
  cur_field_types = dict()
  for attr in dir(FieldType):
    if not attr.startswith('__'):
      cur_field_types[attr] = attr
  cur_field_types.update(new_data_types)
  FieldType = Enum('FieldType', cur_field_types)

def removeDataTypes(removed_data_types: typing.Dict[str, str]):
  global FieldType
  cur_field_types = dict()
  for attr in dir(FieldType):
    if not attr.startswith('__') and attr not in removed_data_types:
      cur_field_types[attr] = attr
  FieldType = Enum('FieldType', cur_field_types)

class Field(object):
  __slots__ = {
    'name': "-> string [REQUIRED]",
    'type': "-> string [REQUIRED] integer|float|string|boolean|date|datetime|time|record",
    'mode': "-> string [OPTIONAL] REQUIRED|NULLABLE|REPEATED: default NULLABLE",
    'fields': "-> list [OPTIONAL IF type = RECORD]",
    'schema_name': "-> string [OPTIONAL]"
  }

  def __init__(self, **attrs):
    self.name = attrs['name']
    self.type = attrs['type']
    self.mode = attrs.get('mode', 'NULLABLE')
    self.schema_name = attrs.get('schema_name')
    self.fields = [
      f if isinstance(f, Field) else Field(**f)
      for f in attrs.get('fields', [])
    ]


  def __repr__(self):
    return "<Field(name={name}, type={type} at {id}>".format(
      id=id(self),
      name=self.name,
      type=self.type
    )
      

  def __eq__(self, other):

    return (
      self.name == other.name
      and self.type == other.type
      and self.mode == other.mode
      and self.fields == other.fields
      and self.schema_name == other.schema_name
    )

  @property
  def path(self):
    if self.schema_name:
      return self.schema_name + "." + self.name
    else:
      return self.name
  

  def new(self, **parts):
    attrs = {attr:getattr(self, attr) for attr in self.__slots__}
    attrs.update(parts)

    return self.__class__(**attrs)

  def to_dict(self):
    return dict(
      name = self.name,
      type = self.type,
      mode = self.mode,
      fields = [f.to_dict() for f in self.fields]
    )


