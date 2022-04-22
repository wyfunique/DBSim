import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from dbsim.query_parser import parse_statement
from dbsim.utils.visualizer import LogicalPlanViz 

standard_sql = """
    SELECT musical.title, musical.year
    FROM 
      (SELECT * 
        FROM 
          (animation JOIN musical ON animation.mid = musical.mid) 
        WHERE 
          animation.mid < 1200
      )
    WHERE musical.year > 1960
"""
ast = parse_statement(standard_sql)
LogicalPlanViz.show(ast, view=True)

extended_syntax_sql = """
    SELECT musical.title, musical.year
    FROM 
      (SELECT * 
        FROM 
          (SELECT * FROM animation, musical WHERE animation.mid = musical.mid) 
        WHERE 
          animation.embedding to [1,2,3,4] < 10
      )
    WHERE musical.year > 1960
"""
ast = parse_statement(extended_syntax_sql)
LogicalPlanViz.show(ast, view=True)
