from ...ast import *
from ...utils import *
from ... import field 
from ... import schema_interpreter
from ... import query_parser_toolbox as toolbox 
from ...compilers import local as main_compiler
from ...adapters.adapter_factory import AdapterFactory

class ExtendedSyntax(object):
  """The abstract class for extended syntax"""
  __slots__ = {
    # plugin syntax symbols and data types
    '_extended_symbols_': '-> str',
    '_extended_clause_keywords_': '-> Dict[SQLClause, str]',  
    '_extended_data_types_': '-> Dict[Type[Const], str]', 
    '_extended_data_types_converter_': '-> Callable[[pandas.Series], [pandas.Series, FieldType]]', 
    # plugin predicate parsers.
    # the second parameter in this attribute presents whether to block errors raised by these parsers.
    # True -> blocking all errors from these parsers; False -> reporting all errors from them.
    # Note that, as an informational exception, ParsingFailure will always be blocked in both cases. 
    '_extended_predicate_parsers_': '-> typing.Tuple[Dict[PredExprLevel, Callable], bool]', 
    # plugin predicate operator executors, used by query compilers
    '_extended_predicate_op_executors_': '-> Dict[Type[SimpleOp], Callable[ [SimpleOp,Schema,DataSet], Callable[[row,ctx],Any] ]]',
    # plugin relational operator schema resolvers (used by schema_interpreter) and executors (used by compilers)
    '_extended_relation_op_schema_': '-> Dict[Type[SuperRelationalOp], Callable[[SuperRelationalOp, DataSet], Schema]]', 
    '_extended_relation_op_executors_': '-> Dict[Type[SuperRelationalOp], Callable[ [DataSet,SuperRelationalOp], Callable[[ctx],Any] ]]', 
  }

  @classmethod
  def addExtendedSymbolsAndKeywords(cls) -> None:
    """
    Adds extended syntax symbols and SQL clause keywords for parser to recognize.
    The symbols are defined by the attribute '_extended_symbols_' of the extended syntax subclass, 
      where each character of the attribute value represents a symbol.
    The SQL clause keywords are defined by the attribute '_extended_clause_keywords_'.
    """
    if hasattr(cls, "_extended_symbols_"):
      toolbox.addSyntaxSymbol(cls._extended_symbols_)
    if hasattr(cls, "_extended_clause_keywords_"):
      toolbox.addClauseKeywords(cls._extended_clause_keywords_)

  @classmethod
  def removeExtendedSymbolsAndKeywords(cls) -> None:
    if hasattr(cls, "_extended_symbols_"):
      toolbox.removeSyntaxSymbol(cls._extended_symbols_)
    if hasattr(cls, "_extended_clause_keywords_"):
      toolbox.removeClauseKeywords(cls._extended_clause_keywords_)

  @classmethod
  def addExtendedPredicateParsers(cls) -> bool:
    """Extends predicate operators"""
    if hasattr(cls, "_extended_predicate_parsers_"):
      if isinstance(cls._extended_predicate_parsers_, tuple):
        block_error = cls._extended_predicate_parsers_[1]
        parsers = cls._extended_predicate_parsers_[0]
      else:
        # do not block the errors by default, 
        # i.e., always raise any exception except ParsingFailure by default
        block_error = False
        parsers = cls._extended_predicate_parsers_
      toolbox.addPredParsers(cls, parsers, block_error)

  @classmethod
  def removeExtendedPredicateParsers(cls) -> bool:
    toolbox.resetPredParsers()

  @classmethod
  def addExtendedDataTypes(cls) -> None:
    """
    Data types are the things like 'int', 'float', 'string', etc.
    To add an extended data type, two attributes need to be defined in the extended syntax subclass:
      (1) a data type name string 
      (2) an AST node class inherited from ast.Const. 
          Any literal of that data type will be parsed to an instance of this AST node.
    Both of them are defined by the '_extended_data_types_' attribute of the extended syntax subclass.   
    """
    if hasattr(cls, "_extended_data_types_"):
      field.addDataTypes({datatype: datatype for datatype in cls._extended_data_types_.values()})
      schema_interpreter.addDataTypes(cls._extended_data_types_)
      main_compiler.addDataTypes(cls._extended_data_types_)
    # "_extended_data_types_converter_" is a function used when reading dataset from file 
    #     to convert string values into objects of the corresponding extended data type.
    if hasattr(cls, "_extended_data_types_converter_"):
      AdapterFactory.addDataTypeConverter(cls, cls._extended_data_types_converter_)
      
  @classmethod
  def removeExtendedDataTypes(cls) -> None:
    if hasattr(cls, "_extended_data_types_"):
      field.removeDataTypes({datatype: datatype for datatype in cls._extended_data_types_.values()})
      schema_interpreter.removeDataTypes(cls._extended_data_types_)
      main_compiler.removeDataTypes(cls._extended_data_types_)
    if hasattr(cls, "_extended_data_types_converter_"):
      AdapterFactory.removeDataTypeConverter(cls)
      
  @classmethod
  def addExtendedPredicateOps(cls) -> bool:
    """Extends predicate operators"""
    if hasattr(cls, "_extended_predicate_op_executors_"):
      main_compiler.addPredicateOps(cls._extended_predicate_op_executors_)

  @classmethod
  def removeExtendedPredicateOps(cls) -> bool:
    if hasattr(cls, "_extended_predicate_op_executors_"):
      main_compiler.removePredicateOps(cls._extended_predicate_op_executors_)

  @classmethod
  def addExtendedRelationOps(cls) -> bool:
    """Extends relational operators"""
    if hasattr(cls, "_extended_relation_op_schema_"):
      schema_interpreter.addRelationOps(cls._extended_relation_op_schema_)    
    if hasattr(cls, "_extended_relation_op_executors_"):
      main_compiler.addRelationOps(cls._extended_relation_op_executors_)

  @classmethod
  def removeExtendedRelationOps(cls) -> bool:
    if hasattr(cls, "_extended_relation_op_schema_"):
      schema_interpreter.removeRelationOps(cls._extended_relation_op_schema_)  
    if hasattr(cls, "_extended_relation_op_executors_"):  
      main_compiler.removeRelationOps(cls._extended_relation_op_executors_)