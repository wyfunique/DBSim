# DBSim

DBSim is an extensible database simulator in pure Python for fast prototyping in-database algorithms. It provides a simulated RDBMS environment, related tools and high extensibility & flexibility for data scientists to fast develop, verify and analyze the prototypes of in-database analytic algorithms. 

Implementing any new in-database algorithm directly in real database kernels without pre-verification may not be a good idea and may cause waste of time. If you agree, try DBSim! Spending a little time on prototyping your in-db algorithms in DBSim to convince yourself before formally making them into real-world DBMS. 

### Features

* Including all the major components of a general RDBMS query engine: SQL parser, relational operators, logical and physical plan generator, query optimizer, etc. 
* High extensibility and flexibility: DBSim provides APIs to extend almost any of the DB components, including but not limited to adding new keywords to query syntax, implementing custom operators, writing custom optimization rules, extending physical plan executors, etc.
* Debugging and analyzing tools: query plan visualizer, GUI, etc.
* Low learning cost for users: writing in Python makes it much easier to learn comparing with the real-world DBMS developed in C/C++/Java. 

### TODO

* Add more optimization rules that are commonly applied by general RDBMS
* Implement Volcano-style optimizer (currently DBSim only supports heuristic (rule-based) optimizer)
* Develop rule definition language to support simpler claims of custom optimization rules
* Integrate more performance analyzing and visualization tools 

### Install 

DBSim requires Python 3.6+ . To install necessary dependencies, run the following command:

```
pip install -r requirements.txt
```

### Run the demo

```
python demo.py
```

### Examples

We use some basic examples to briefly present DBsim here. Please see `dbsim/examples/` and `dbsim/tests/` for more detailed examples.

1. **Execute a query end-to-end**

   (1) without query optimizer

   ```python
   from dbsim import dataset as ds
   from dbsim.tests.fixtures.demo_adapter import DemoAdapter
   from dbsim.query_parser import parse_statement
   from dbsim.query import Query
   
   dataset = ds.DataSet()
   dataset.add_adapter(DemoAdapter())
   sql = """
       SELECT musical.title, musical.year
       FROM 
         (SELECT * 
           FROM 
             (SELECT * FROM animation, musical WHERE animation.mid = musical.mid) 
           WHERE 
             animation.mid < 3000
         )
       WHERE musical.year > 1960
   """
   for row in Query(dataset, parse_statement(sql)):
     print(row)
   ```

   (2) with query optimizer

   ```python
   from dbsim import dataset as ds
   from dbsim.tests.fixtures.demo_adapter import DemoAdapter
   from dbsim.query_parser import parse_statement
   from dbsim.query import Query
   from dbsim.planners import rules
   from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
   
   dataset = ds.DataSet()
   dataset.add_adapter(DemoAdapter())
   planner = HeuristicPlanner(max_limit = float('Inf'))
   planner.addRule(rules.FilterMergeRule())
   planner.addRule(rules.FilterPushDownRule())
   planner.addRule(rules.Selection_SimSelection_Swap_Rule())
   
   sql = """
       SELECT musical.title, musical.year
       FROM 
         (SELECT * 
           FROM 
             (SELECT * FROM animation, musical WHERE animation.mid = musical.mid) 
           WHERE 
             animation.mid < 3000
         )
       WHERE musical.year > 1960
   """
   for row in Query(dataset, parse_statement(sql), optimizer=planner):
     print(row)
   ```

   

2. **Parse a query into abstract syntax tree(AST) and visualize the AST**

   ```python
   from dbsim.query_parser import parse_statement
   from dbsim.utils.visualizer import LogicalPlanViz 
   
   sql = """
       SELECT musical.title, musical.year
       FROM musical
       WHERE musical.year > 1960
   """
   ast = parse_statement(sql)
   LogicalPlanViz.show(ast, view=True)
   ```

   

3. **Manually optimize a logical plan and visualize the resulting plan** 

   ```python
   from dbsim import dataset as ds
   from dbsim.tests.fixtures.demo_adapter import DemoAdapter
   from dbsim.query_parser import parse_statement
   from dbsim.query import Query
   from dbsim.planners import rules
   from dbsim.planners.heuristic.heuristic_planner import HeuristicPlanner
   from dbsim.utils.visualizer import LogicalPlanViz 
   
   dataset = ds.DataSet()
   dataset.add_adapter(DemoAdapter())
   
   planner = HeuristicPlanner(max_limit = float('Inf'))
   planner.addRule(rules.FilterMergeRule())
   planner.addRule(rules.FilterPushDownRule())
   planner.addRule(rules.Selection_SimSelection_Swap_Rule())
   
   sql = """
       SELECT musical.title, musical.year
       FROM 
         (SELECT * 
           FROM 
             (SELECT * FROM animation, musical WHERE animation.mid = musical.mid) 
           WHERE 
             animation.mid < 3000
         )
       WHERE musical.year > 1960
   """
   plan = Query(dataset, parse_statement(sql)).getPlan()
   best_plan = planner.findBestPlan(plan)
   LogicalPlanViz.show(best_plan, view=True)
   ```

   

4. **Note: ** We already extended the standard SQL syntax in DBSim as examples to show its extensibility. Please see the documentation for further tutorials. But at this step you can quickly try such an extended-SQL-syntax query in any of the code snippets above:

   ```python
   sql = """
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
   ```

   

5. **More examples:**

   To try more examples, run the following command (DO NOT cd to `dbsim/` or `dbsim/examples`, just run the command here, i.e., in the root path of this repo):

   ```bash
   python dbsim/examples/<example_filename>.py
   ```

   To run the tests in `dbsim/tests/`, you can use Pytest or manually call each testing function.

### Run the GUI

In the repo root path, run following command to start the GUI:

```bash
python dbsim/gui/backend/server.py
```

Then open your browser and go to the link `localhost:8888`. Please read our paper [Extensible Database Simulator for Fast Prototyping In-Database Algorithms](https://arxiv.org/abs/2204.09819) for more details on how to use the GUI. The GUI is only tested on Microsoft Edge, and currently we do not guarantee its compatibility with other browsers.

**Note:** 

(1) To run a query, you need to let the query input box lose focus, like clicking anywhere outside the box.

(2) We provide example datasets and query in the GUI when it is started. You can try with them or use your own datasets and queries.

### Documentation

See the Wiki of this repo.

### Acknowledgement 

DBSim is developed based on Splicer ([trivio/splicer: Splicer - adds relation querying (SQL) to any python project (github.com)](https://github.com/trivio/splicer)). And we also borrowed some ideas from Apache Calcite.

### Citation

If you use this codebase, or otherwise found our work valuable, please cite:

```
@misc{https://doi.org/10.48550/arxiv.2204.09819,
  doi = {10.48550/ARXIV.2204.09819},
  url = {https://arxiv.org/abs/2204.09819},
  author = {Wang, Yifan and Wang, Daisy Zhe},
  title = {Extensible Database Simulator for Fast Prototyping In-Database Algorithms},
  publisher = {arXiv},
  year = {2022},
  copyright = {arXiv.org perpetual, non-exclusive license}
}
```

