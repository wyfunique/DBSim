import os
import pandas as pd
import numpy
import inspect
import typing
from typing import Dict, Type, Callable

from . import Adapter
from .dataframe_adapter import DataFrameAdapter
from ..utils import *
from ..utils.logger import Logger
from .. import field 

logger = Logger.general_logger

class AdapterFactory(object):
  
  supported_filetypes = set([
    "csv", "tsv",
  ])
  
  pandasType2FieldType = {
    numpy.integer: field.FieldType.INTEGER,
    numpy.floating: field.FieldType.FLOAT,
    numpy.character: field.FieldType.STRING,
    object: field.FieldType.STRING, # pandas stores text as type 'object'
    numpy.bool_: field.FieldType.BOOLEAN,
    numpy.datetime64: field.FieldType.DATETIME,
  }

  extendedSyntaxDataTypeConverters: Dict[Type, Callable] = dict()

  @classmethod
  def addDataTypeConverter(cls, extended_syntax: Type, datatype_converter: Callable):
    ERROR_IF_FALSE(
      isSubclassByClassName(extended_syntax, "ExtendedSyntax"), 
      "method 'addDataTypeConverter' only accepts an ExtendedSyntax subclass as the first argument ('{}' received)"\
        .format(type(extended_syntax)) 
    )
    if extended_syntax in cls.extendedSyntaxDataTypeConverters:
      logger.warn("The datatype converter of ExtendedSyntax '{}' is already registered.".format(extended_syntax))
    cls.extendedSyntaxDataTypeConverters[extended_syntax] = datatype_converter
  
  @classmethod
  def removeDataTypeConverter(cls, extended_syntax: Type):
    ERROR_IF_FALSE(
      isSubclassByClassName(extended_syntax, "ExtendedSyntax"), 
      "method 'removeDataTypeConverter' only accepts an ExtendedSyntax subclass as the first argument ('{}' received)"\
        .format(type(extended_syntax)) 
    )
    if extended_syntax in cls.extendedSyntaxDataTypeConverters:
      logger.warn("The datatype converter of ExtendedSyntax '{}' is not registered.".format(extended_syntax))
    del cls.extendedSyntaxDataTypeConverters[extended_syntax] 

  @classmethod
  def convertType(cls, df_column: pd.Series) -> typing.Tuple[pd.Series, field.FieldType]:
    """
    Parses the data type of current dataframe column and converts 
      (1) the pandas data type to proper FieldType, and
      (2) the values in this column to proper objects.
    For example, given a column with string values where each looks like '[a1, a2, a3, ...]', 
      each value will be converted into a np.array([a1, a2, a3, ...]) 
      since the value '[a1, a2, a3, ...]' is a vector representation.
    And finally the corresponding FieldType will be returned, 
      like "FieldType.VECTOR" will be returned in the above example.    

    Parameters
    ------------
    df_column: a column (Series) of the dataframe

    Returns
    ------------
    (1) The converted dataframe column (Series), and 
    (2) the FieldType to which the data type was converted
    """
    df_col_dtype = df_column.dtype 
    if df_col_dtype == object:
      # The current dataframe column stores string type values.
      # Those values may be pure strings, or some complex objects
      #   that are not recognized by pandas.
      for syntax in cls.extendedSyntaxDataTypeConverters:
        converter = cls.extendedSyntaxDataTypeConverters[syntax]
        try:
          converted_column, converted_type = converter(df_column)
          if isinstance(converted_type, field.FieldType):
            # conversion succeeded
            return converted_column, converted_type
          else:
            continue
        except Exception as e:
          # the current converter cannot recognize the data type of the current values, 
          #   so continue to use the next converter to have a try.
          logger.error(e)
          continue
      # No extended syntax data type converter can recognize the current data type, 
      #   so keep it as string by default.
      return df_column, cls.pandasType2FieldType[object]
    for super_type in cls.pandasType2FieldType:
      if super_type != object and numpy.issubdtype(df_col_dtype, super_type):
        return df_column, cls.pandasType2FieldType[super_type]
    raise RuntimeError("No matched FieldType to pandas dtype '{}'".format(df_col_dtype))

  @classmethod
  def fromFile(cls, filepath: str, ds_name: str = None) -> Adapter:
    ext = os.path.splitext(filepath)[1]
    # ext looks like ".csv", ".txt", etc.
    if len(ext) > 0 and ext[0] == '.':
      ext = ext[1:]
    ERROR_IF_FALSE(
      ext in cls.supported_filetypes,
      "File type '{}' not supported currently (supported types: {})".format(ext, ', '.join(list(cls.supported_filetypes)))
    )
    if ds_name is None:
      ds_name = os.path.basename(filepath)
      if '.' in ds_name:
        ds_name = ds_name.split('.')[0]
    # infer the separator without being specified by users 
    # note that "iterator = True" will keep the input file open, 
    #   so here we must manually close the file. 
    tmp_reader = pd.read_csv(filepath, sep = None, iterator = True)
    inferred_sep = tmp_reader._engine.data.dialect.delimiter
    tmp_reader.close() # manually close the csv file 

    df = pd.read_csv(filepath, sep = inferred_sep, encoding = "utf-8")
    fields = []
    for col_name in df.columns:
      converted_column, converted_type = cls.convertType(df[col_name])
      df[col_name] = converted_column
      fields.append(dict(name = col_name, type = converted_type))
    schema = dict(fields = fields)
    return DataFrameAdapter(**{ds_name: dict(schema = schema, dataframe = df)})


