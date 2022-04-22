class FunctionWapper:
    def __init__(self, name, func_body, returns=None):
            self.name = name
            self.func_body = func_body
            self.returns = returns
            
    def __call__(self, *args):
        return self.func_body(*args)