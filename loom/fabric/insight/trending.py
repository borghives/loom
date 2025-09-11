import numpy as np
from pydantic import BaseModel, Field

from loom.fabric.insight.pdf_value import PdfValue
from loom.fabric.insight.regression import Regression
from loom.fabric.insight.spread import Spread


class Trending(BaseModel):
    """
    A composite model that aggregates multiple statistical views (spread, regression, PDF)
    for a single time-series dataset to provide a comprehensive picture of its trend.
    """
    range       : Spread        = Field(description="The statistical spread of the value.")
    regression  : Regression    = Field(description="The linear regression trend of the value.")
    likely      : PdfValue      = Field(description="The Probability Density Function analysis of the value.")

    @classmethod
    def calculate(cls, x: np.ndarray, data_points: np.ndarray) -> "Trending":
        """
        Calculates all trend metrics for the given dataset.
        """
        return cls(
            range=Spread.calculate(data_points),
            regression=Regression.calculate(x, data_points),
            likely=PdfValue.calculate(data_points)
        )