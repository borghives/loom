
import numpy as np
from pydantic import BaseModel, Field
from scipy import stats


class Regression(BaseModel):
    """
    Calculates and holds the results of a linear regression analysis.
    This is used to determine the trend (slope) of a variable over another, typically time.
    """
    slope       : float = Field(description="Slope of the regression line, indicating the rate of change.")
    intercept   : float = Field(description="Y-intercept of the regression line.")
    rvalue      : float = Field(description="The Pearson correlation coefficient (r-value). Measures the strength and direction of the linear relationship.")
    pvalue      : float = Field(description="The p-value for a hypothesis test where the null hypothesis is that the slope is zero. A low p-value (< 0.05) indicates a statistically significant non-zero slope.")
    std_err     : float = Field(description="The standard error of the estimated slope.")

    @classmethod
    def calculate(cls, x: np.ndarray, data_points: np.ndarray) -> "Regression":
        """
        Performs a linear regression on the given data points.

        Args:
            x: The independent variable (e.g., time).
            data_points: The dependent variable.

        Returns:
            A Regression instance with the calculated line-fit parameters.
        """
        # The x.min() == x.max() check prevents a crash inside linregress
        # which occurs if all x values are identical (division by zero).
        if len(x) < 2 or len(data_points) < 2:
            return cls(slope=0, intercept=0, rvalue=0, pvalue=1, std_err=0)
        
        # Filter out NaNs and Infs
        mask = np.isfinite(x) & np.isfinite(data_points)
        x_clean = x[mask]
        y_clean = data_points[mask]

        if len(x_clean) < 2 or x_clean.min() == x_clean.max():
             return cls(slope=0, intercept=0, rvalue=0, pvalue=1, std_err=0)

        # Perform linear regression and get the result as a named tuple.
        result = stats.linregress(x_clean, y_clean)
        
        # Create an instance of the class using attributes from the result object.
        return cls(slope=result.slope, intercept=result.intercept, rvalue=result.rvalue, pvalue=result.pvalue, std_err=result.stderr)
