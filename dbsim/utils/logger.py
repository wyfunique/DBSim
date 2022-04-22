import logging
import sys
  
class Logger(object):
  # general logger for global use
  general_logger = logging.getLogger("general_logger")
  _general_handler = logging.StreamHandler(stream=sys.stdout)
  _general_format = logging.Formatter(
    fmt='[%(asctime)s] %(levelname)s - %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p'   
  )
  _general_handler.setFormatter(_general_format)
  #_general_handler.setLevel(logging.INFO)
  _general_handler.setLevel(logging.ERROR)
  general_logger.addHandler(_general_handler)

  # finer logger, mostly used for debugging
  finer_logger = logging.getLogger("finer_logger")
  _finer_handler = logging.StreamHandler(stream=sys.stderr)
  _finer_format = logging.Formatter(
    fmt='[%(asctime)s] %(levelname)s - %(module)s.%(funcName)s: %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p'   
  )
  _finer_handler.setFormatter(_finer_format)
  # switch between these two levels when necessary:
  #   if debugging, use DEBUG level;
  #   if not, using WARNING or ERROR level is enough.
  #_finer_handler.setLevel(logging.DEBUG)
  _finer_handler.setLevel(logging.ERROR)
  finer_logger.addHandler(_finer_handler)

