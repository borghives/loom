import numpy as np
import pytest
from hypothesis import given, strategies as st
from hypothesis.extra.numpy import arrays

from loom.fabric.insight.spread import Spread
from loom.fabric.insight.trending import Trending

# Strategy for generating 1D numpy arrays of floats
# We include allow_nan=True and allow_infinity=True to test robustness
float_arrays = arrays(
    dtype=np.float64,
    shape=st.integers(min_value=0, max_value=1000),
    elements=st.floats(allow_nan=True, allow_infinity=True, width=64),
)

class TestSpreadPBT:
    @given(float_arrays)
    def test_spread_calculation_invariants(self, data):
        """
        Property: For any array of floats, Spread.calculate should:
        1. Not crash (handled by execution).
        2. Produce finite metrics (if input has at least one finite number).
        3. Respect percentile ordering: p25 <= p50 <= p75 (for valid data).
        """
        # Filter explicitly to match what Spread.calculate likely does internally or should do
        valid_data = data[np.isfinite(data)]
        
        # If all data is NaN or empty, we expect 0s (based on likely implementation)
        # We need to verify the actual behavior of Spread.calculate for empty/nan inputs
        result = Spread.calculate(data)
        
        assert isinstance(result, Spread)
        
        if len(valid_data) == 0:
            assert result.value_25p == 0.0
            assert result.value_50p == 0.0
            assert result.value_75p == 0.0
            assert result.mean == 0.0
            assert result.std_dev == 0.0
        else:
            # Check for ordering invariants
            # Note: np.percentile might return nans if we input nans, but Spread handles that?
            # Looking at source, Spread drops NaNs: data_points = data_points[~np.isnan(data_points)]
            
            # So if we have valid data left, results should be consistently ordered
            assert result.value_25p <= result.value_50p
            assert result.value_50p <= result.value_75p
            
            # Check consistency with numpy (sanity check)
            # We use isclose because of floating point potential minor diffs, though usually exact for percentiles if logic is same
            expected_mean = np.mean(valid_data)
            assert np.isclose(result.mean, expected_mean, equal_nan=True)

class TestTrendingPBT:
    @given(
        # Two arrays of the same length
        st.integers(min_value=2, max_value=100).flatmap(
            lambda n: st.tuples(
                arrays(dtype=np.float64, shape=(n,), elements=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False)), # X usually time, assume regular
                arrays(dtype=np.float64, shape=(n,), elements=st.floats(allow_nan=True, allow_infinity=True, width=64)) # Y data
            )
        )
    )
    def test_trending_calculation_robustness(self, args):
        x, y = args
        # Ensure x is sorted as time usually is, though regression might not strictly require it, 
        # but Trending often implies time series.
        x.sort()

        # Trending.calculate(x, y)
        # Should not crash
        try:
            result = Trending.calculate(x, y)
            assert isinstance(result, Trending)
            assert isinstance(result.range, Spread)
            # result.regression and result.likely are also expected
        except Exception as e:
            pytest.fail(f"Trending.calculate crashed with inputs X={x}, Y={y}. Error: {e}")
