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

class Percentile(AccOpExpression):
    def __init__(self, input: Expression, p: list[float]) -> None:
        self.input = input
        self.p = p

    @property
    def repr_value(self):
        return {
            "$percentile": {
                "input": self.input,
                "p": self.p,
                "method": "approximate"
            }
        }

class ArrayElemAt(AccOpExpression):
    def __init__(self, input: Expression, index: int) -> None:
        self.input = input
        self.index = index

    @property
    def repr_value(self):
        return {
            "$arrayElemAt": [self.input, self.index]
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