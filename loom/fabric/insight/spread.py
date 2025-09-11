
import numpy as np
from pydantic import BaseModel, Field


class Spread(BaseModel):
    """
    Calculates and holds key statistical spread metrics for a dataset.
    This includes percentiles, standard deviation, and mean.
    """
    value_25p : float = Field(description="25th percentile value")
    value_50p : float = Field(description="50th percentile value (median)")
    value_75p : float = Field(description="75th percentile value")
    std_dev   : float = Field(description="Standard Deviation")
    mean      : float = Field(description="Mean")

    @classmethod
    def calculate(cls, data_points: np.ndarray) -> "Spread":
        """
        Calculates spread metrics from a numpy array of data points.
        """

        data_points = data_points[~np.isnan(data_points)]

        if len(data_points) == 0:
            return cls(value_25p=0, value_50p=0, value_75p=0, std_dev=0, mean=0)
        

            
        value_25p, value_50p, value_75p = np.percentile(data_points, [25, 50, 75])
        std_dev = np.std(data_points)
        mean = np.mean(data_points)
        return cls(
            value_25p=float(value_25p), 
            value_50p=float(value_50p), 
            value_75p=float(value_75p), 
            std_dev=float(std_dev),
            mean=float(mean)
        )