from loom.info.expression import AccOpExpression, Expression

# An accumulator operation that computes the accumulation value
# https://www.mongodb.com/docs/manual/reference/mql/accumulators/

class Median(AccOpExpression):
    def __init__(self, input: Expression) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$median": {
                "input": self.input,
                "method": "approximate"
            }
        }

class Sum(AccOpExpression):
    def __init__(self, input: Expression | int ) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$sum": self.input
        }
    
class Avg(AccOpExpression):
    def __init__(self, input: Expression ) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$avg": self.input
        }
    
class Min(AccOpExpression):
    def __init__(self, input: Expression ) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$min": self.input
        }
    
class Max(AccOpExpression):
    def __init__(self, input: Expression ) -> None:
        self.input = input

    @property
    def repr_value(self):
        return {
            "$max": self.input
        }