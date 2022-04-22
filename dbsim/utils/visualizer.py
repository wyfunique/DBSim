import queue
import os
import graphviz
from dbsim.ast import * 
from . import *

class LogicalPlanViz:
    """
    Visulizer for the given logical plan or AST
    """
    def __init__(self) -> None:
        pass
    
    @classmethod
    def predicateToStr(cls, bool_op: BinaryOp) -> str:
        return str(bool_op)

    @classmethod
    def getVizNodeTitle(cls, ast_node: Expr, show_cost_factor: bool=False, show_num_input_rows: bool=False) -> str:
        title = str(ast_node)
        if show_cost_factor:
            cost_factor = ast_node.getCostFactor()
            title += (' cf: ' + str(cost_factor)) if cost_factor else ''
        if show_num_input_rows:
            num_input_rows = ast_node.getNumInputRows()
            title += (' nr: ' + str(num_input_rows)) if num_input_rows else ''
        return title
        
    @classmethod
    def show(cls, root: Expr, show_cost_factor: bool=False, 
            show_num_input_rows: bool=False, img_save_dir: str=os.path.dirname(__file__), 
            img_name: str="LogicalPlan", view: bool=False):
        dataset_node_shape, op_node_shape = 'box', 'oval'
        graph = graphviz.Graph(img_name, comment='The logical execution plan', format='png')
        graph.format = 'svg'
        # BFS to traverse the input AST
        q = queue.Queue() # q stores triples (ast node, its viz node id, its parent viz node id)
        next_viz_node_id = 0
        q.put((root, next_viz_node_id, -1))
        node_title = cls.getVizNodeTitle(root, show_cost_factor, show_num_input_rows)
        node_op_name, node_predicate = node_title.split(': ')
        graph.node(
            name = str(next_viz_node_id), 
            label = node_op_name + '\\n' + node_predicate, 
            shape = op_node_shape
        )
        next_viz_node_id += 1 # the next id to be assigned
        while not q.empty():
            cur_node, cur_viz_node_id, parent_viz_node_id = q.get()            
            # connect the current viz node with its parent 
            if parent_viz_node_id >= 0:
                graph.edge(str(parent_viz_node_id), str(cur_viz_node_id))
            # create viz node(s) for its children without edges
            if (isinstance(cur_node, UnaryOp) or isinstance(cur_node, RelationalOp)) and not isinstance(cur_node, (LoadOp, Relation)):
                child_node = cur_node.relation
                if child_node is not None:
                    node_title = cls.getVizNodeTitle(child_node, show_cost_factor, show_num_input_rows)
                    node_op_name, node_predicate = node_title.split(': ')
                    graph.node(
                        name = str(next_viz_node_id), 
                        label = node_title if isinstance(child_node, (Relation, LoadOp)) else node_op_name + '\\n' + node_predicate, 
                        shape = dataset_node_shape if isinstance(child_node, (Relation, LoadOp)) else op_node_shape
                    )
                    q.put((child_node, next_viz_node_id, cur_viz_node_id))  
                    next_viz_node_id += 1
            elif isinstance(cur_node, BinaryOp) or isinstance(cur_node, BinRelationalOp):
                if isinstance(cur_node, BinaryOp):
                    left_child = cur_node.lhs
                    right_child = cur_node.rhs
                elif isinstance(cur_node, BinRelationalOp):
                    left_child = cur_node.left
                    right_child = cur_node.right
                if left_child is not None:
                    node_title = cls.getVizNodeTitle(left_child, show_cost_factor, show_num_input_rows)
                    node_op_name, node_predicate = node_title.split(': ')
                    graph.node(
                        name = str(next_viz_node_id), 
                        label = node_title if isinstance(left_child, (Relation, LoadOp)) else node_op_name + '\\n' + node_predicate, 
                        shape = dataset_node_shape if isinstance(left_child, (Relation, LoadOp)) else op_node_shape
                    )
                    q.put((left_child, next_viz_node_id, cur_viz_node_id))  
                    next_viz_node_id += 1
                    node_title = cls.getVizNodeTitle(right_child, show_cost_factor, show_num_input_rows)
                    node_op_name, node_predicate = node_title.split(': ')
                    graph.node(
                        name = str(next_viz_node_id), 
                        label = node_title if isinstance(right_child, (Relation, LoadOp)) else node_op_name + '\\n' + node_predicate, 
                        shape = dataset_node_shape if isinstance(right_child, (Relation, LoadOp)) else op_node_shape
                    )
                    q.put((right_child, next_viz_node_id, cur_viz_node_id))  
                    next_viz_node_id += 1
        graph.render(directory=img_save_dir, view=view)