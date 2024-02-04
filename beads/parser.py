from lark import Lark
import ast


def find_variables(expression):
    class VariableVisitor(ast.NodeVisitor):
        def __init__(self):
            self.variables = set()

        def visit_Name(self, node):
            if isinstance(node.ctx, ast.Load):
                self.variables.add(node.id)

    tree = ast.parse(expression)
    visitor = VariableVisitor()
    visitor.visit(tree)
    return visitor.variables


def find_node_data(tree, filter_string, multiple=False):
    nodes = list(tree.find_data(filter_string))
    if len(nodes) == 0:
        return None
    
    node = nodes[0]
    if len(node.children) == 0:
        return None
    
    if multiple:
        return nodes[0].children
    else:
        return nodes[0].children[0]


def process_condition(condition_string):
    condition_string = str(condition_string).strip()
    variables = find_variables(condition_string)
    if 'this' not in variables:
        raise Exception("this.* reference is not found")
    if len(variables) == 1:
        return condition_string.replace('this.', ''), []
    if len(variables) == 2:
        second_node = [v for v in variables if v != 'this'][0]
        
        return condition_string.replace(
            'this.', 'B_'
        ).replace(
            f'{second_node}.', 'A_'
        ), [second_node]
    else:
        raise Exception(f"Wrong number of variables: {variables}")


def index_query(query, base_index):
    if isinstance(query, list):
        return [index_query(e, f'{base_index}_{i}') for i, e in enumerate(query)]

    elif isinstance(query, dict) and any(key in query for key in ['not', 'and', 'or']):
        operator = list(query.keys())[0]  # Получаем оператор ('not', 'and', 'or')
        return (f'{base_index}_{operator}', {
            operator: [
                index_query(arg, f'{base_index}_{operator}_{i}') 
                for i, arg in enumerate(query[operator])
            ]
        })

    else:
        return (base_index, query)


class Parser:
    def __init__(self):
        self.grammar = Lark('''
start: node ("=>" node)*
node: simple_node | not_node | nary_node
not_node: "not" "{" start "}"
nary_node: NARY_OPERATOR ("{" start "}")+
simple_node: alias? (parent "->")? event_name conditions
alias: NAME ":"
parent: NAME
event_name: NAME
conditions: ("[" CONDITION "]")*

CONDITION: /[^\]]+/
NARY_OPERATOR: "and" | "or"

%import common.CNAME -> NAME
%import common.NEWLINE
%import common.WS

%ignore WS
%ignore NEWLINE
        ''')

    def _create_simple_node(self, node, node_index):
        aliases = self.aliases

        node_dict = {
            'node_conditions': [],
            'order_conditions': []
        }
        
        node_alias = find_node_data(node, 'alias')
        if node_alias:
            node_dict['alias'] = node_alias
            aliases[node_alias] = node_index
        
        node_type = find_node_data(node, 'event_name')
        if node_type:
            node_dict['node_conditions'].append(f'type == "{node_type}"')
            
        node_parent = find_node_data(node, 'parent')
        if node_parent:
            # parent_index = aliases[node_parent]
            node_dict['order_conditions'].append({
                'node_used': node_parent,
                'condition': 'A_start_id == B_end_id'
            })
            
        node_conditions = find_node_data(node, 'conditions', multiple=True)
        if node_conditions:
            for condition in node_conditions:
                pandas_condition, condition_nodes = process_condition(condition)
                if len(condition_nodes) == 0:
                    node_dict['node_conditions'].append(pandas_condition)
                if len(condition_nodes) == 1:
                    condition_node_alias = condition_nodes[0]
                    # condition_node_index = aliases[condition_node_alias]
                    node_dict['order_conditions'].append({
                        'node_used': condition_node_alias,
                        'condition': pandas_condition
                    })

        return node_dict

    def _parse(self, tokens):
        aliases = self.aliases

        print(tokens)
        print('--------------')
        query = []

        for node_index, node in enumerate(tokens.children):
            node_query_type = node.children[0].data
    
            if node_query_type == 'not_node':
                subquery_tokens = node.children[0].children[0]
                subquery = self._parse(subquery_tokens)
                query.append({
                    'not': subquery
                })

            if node_query_type == 'nary_node':
                subqueries = []
                nary_operator = str(node.children[0].children[0])

                for subquery_tokens in node.children[0].children[1:]:
                    subquery = self._parse(subquery_tokens)
                    subqueries.append(subquery)

                query.append({
                    nary_operator: subqueries
                })

            elif node_query_type == 'simple_node':
                node_dict = self._create_simple_node(node, node_index)
                query.append(node_dict)

        return query

    def parse(self, query):
        self.aliases = {}
        tokens = self.grammar.parse(query)
        query = self._parse(tokens)
        indexed_query = index_query(query, 'e')

        return indexed_query