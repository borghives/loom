from datetime import datetime
from typing import Optional
import numpy as np
import pandas as pd
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

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
    
class TrendingFrame(BaseModel):
    """
    Represents the analysis of an option contract's market data at a specific moment in time.
    It analyzes the trend of the contract's market value and its percentage change over a future/past window.
    """
    ceiling_time    : datetime              = Field(description="The timestamp of the latest data point included in this frame.")
    floor_time      : datetime              = Field(description="The timestamp of the earliest data point included in this frame.")
    data_count      : int                   = Field(description="The number of data points used in the analysis.")

    model_config = ConfigDict(extra='allow')
    

    @classmethod
    def consume_data(cls, df: pl.DataFrame | pd.DataFrame, target_fields: list[str], time_field: str = 'time_distance_hr') -> Optional["TrendingFrame"]:
        """
        Factory method to create a FutureMarketFrame from a DataFrame of option chain data.
        """
        if time_field not in df.columns:
            ValueError(f"DataFrame must contain a '{time_field}' column representing time distances.")
        
        data_count = len(df)
        if len(df) <= 1:
            return None
        
        time_data = df[time_field].to_numpy() # time distance from a context moment

        trending_value = {}
        for field in target_fields:

            trending_value[field] = Trending.calculate(time_data, df[field].to_numpy())
        
        max_time = df['updated_time'].max()
        min_time = df['updated_time'].min()

        assert isinstance(max_time, datetime)
        assert isinstance(min_time, datetime)   

        return cls(
            ceiling_time    = max_time,
            floor_time      = min_time,
            data_count      = data_count,
            **trending_value
        )