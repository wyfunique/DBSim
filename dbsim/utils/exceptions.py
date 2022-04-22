class SQLSyntaxError(Exception):
    """Exception raised for invalid SQL syntax."""

    def __init__(self, message="Invalid SQL syntax"):
        self.message = message
        super().__init__(self.message)
    
class PlannerInternalError(Exception):
    """Exception raised for internal errors in planner."""

    def __init__(self, message="Planner internal error"):
        self.message = message
        super().__init__(self.message)
    
class RuleImplementError(Exception):
    """Exception raised for problematic implementation of optimization rules."""

    def __init__(self, message="Invalid rule implementation"):
        self.message = message
        super().__init__(self.message)

class FieldNotFoundError(Exception):
    """Exception raised for looking for a non-existing field(column)."""

    def __init__(self, message="Field not found"):
        self.message = message
        super().__init__(self.message)

class AmbigousFieldError(Exception):
    """Exception raised for ambigous field."""

    def __init__(self, message="ambigous field"):
        self.message = message
        super().__init__(self.message)

class ExtensionInternalError(Exception):
    """Exception raised for internal errors in extensions (like extended operators)."""

    def __init__(self, message="Extension internal error"):
        self.message = message
        super().__init__(self.message)

class ExtendedSyntaxError(Exception):
    """Exception raised for invalid SQL syntax."""

    def __init__(self, message="Invalid SQL syntax"):
        self.message = message
        super().__init__(self.message)

class RegistryError(Exception):
    """Exception raised in registry."""

    def __init__(self, message="Invalid registry"):
        self.message = message
        super().__init__(self.message)

class ParsingFailure(Exception):
    """Informational exception raised when the current predicate parser failed to parse the current tokens."""

    def __init__(self, message="Predicate Parsing Failure"):
        self.message = message
        super().__init__(self.message)
