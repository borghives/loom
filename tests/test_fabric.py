
import pytest
from datetime import datetime, timedelta
from bson import ObjectId
from loom.fabric.moment import Moment, MomentWindow
from loom.time.timeframing import DailyFrame
from loom.time.util import to_utc_aware, EASTERN_TIMEZONE

@pytest.fixture
def moments():
    """Provides a sorted list of entangled Moment objects for testing."""
    now = to_utc_aware(datetime.now())
    return [
        Moment(symbol="TEST", updated_time=now - timedelta(days=4), _id=ObjectId()),
        Moment(symbol="TEST", updated_time=now - timedelta(days=3), _id=ObjectId()),
        Moment(symbol="TEST", updated_time=now - timedelta(days=2), _id=ObjectId()),
        Moment(symbol="TEST", updated_time=now - timedelta(days=1), _id=ObjectId()),
        Moment(symbol="TEST", updated_time=now, _id=ObjectId()),
    ]

# --- Moment Tests ---

def test_moment_creation_and_properties():
    """Tests basic Moment creation and property access."""
    now = datetime.now()
    moment = Moment.create(time=now, symbol="test_symbol", value=123)
    
    assert moment.symbol == "TEST_SYMBOL"  # StrUpper works
    assert moment.time == to_utc_aware(now)
    assert moment.date_str == to_utc_aware(now).strftime("%Y-%m-%d")
    assert moment.value == 123 #type: ignore # extra fields are allowed

def test_moment_comparison(moments):
    """Tests the comparison operators on Moment objects."""
    m1 = moments[0]
    m2 = moments[1]
    m1_copy = Moment.create(time=m1.time, symbol="TEST")

    assert m1 < m2
    assert m2 > m1
    assert m1 != m2
    assert m1 == m1_copy
    assert m1 <= m2
    assert m1 <= m1_copy
    assert m2 >= m1
    assert m1_copy >= m1


# --- MomentWindow Tests ---

def test_moment_window_initialization(moments):
    """Tests the initialization of MomentWindow."""
    # Test with a list of moments
    window = MomentWindow(moments=moments)
    assert len(window) == 5
    assert window.symbol == "TEST"
    assert window.moments[0] < window.moments[1]

    # Test with a different symbol (should be filtered out)
    mixed_moments = moments + [Moment(symbol="OTHER", updated_time=datetime.now(), _id=ObjectId())]
    window = MomentWindow(moments=mixed_moments, symbol="TEST")
    assert len(window) == 5

    # Test with non-entangled moment (should be filtered out)
    non_entangled_moment = Moment(symbol="TEST", updated_time=datetime.now())
    window = MomentWindow(moments=moments + [non_entangled_moment])
    assert len(window) == 5
    
    # Test empty initialization
    empty_window = MomentWindow(symbol="EMPTY")
    assert len(empty_window) == 0
    assert empty_window.symbol == "EMPTY"

def test_moment_window_dunder_methods(moments):
    """Tests the dunder methods of MomentWindow."""
    window = MomentWindow(moments=moments)
    
    # __len__
    assert len(window) == 5
    
    # __iter__
    assert all(isinstance(m, Moment) for m in window)
    
    # __getitem__ (int)
    assert window[0] == moments[0]
    
    # __getitem__ (slice)
    sub_window = window[1:3]
    assert isinstance(sub_window, MomentWindow)
    assert len(sub_window) == 2
    assert sub_window[0] == moments[1]
    
    # __str__
    assert str(window) == f"TEST : {moments[0].date_str} - {moments[-1].date_str}"
    assert str(MomentWindow()) == "[empty]"

def test_sliding_window(moments):
    """Tests the sliding_window method."""
    window = MomentWindow(moments=moments)
    
    windows = list(window.sliding_window(3))
    assert len(windows) == 3
    
    # Check first window
    assert len(windows[0]) == 3
    assert windows[0][0] == moments[0]
    assert windows[0][2] == moments[2]
    
    # Check last window
    assert len(windows[2]) == 3
    assert windows[2][0] == moments[2]
    assert windows[2][2] == moments[4]

def test_sliding_time_cone(moments):
    """Tests the sliding_time_cone method."""
    window = MomentWindow(moments=moments)
    
    cones = list(window.sliding_time_cone(past_size=3, future_size=2))
    assert len(cones) == 3
    
    # First cone
    past, future = cones[0]
    assert len(past) == 3
    assert len(future) == 2
    assert past[0] == moments[0]
    assert future[1] == moments[4]
    
    # Last cone (future should be truncated)
    past, future = cones[2]
    assert len(past) == 3
    assert len(future) == 0
    assert past[0] == moments[2]
    assert past[2] == moments[4]

def test_get_moments(moments):
    """Tests filtering moments by time."""
    window = MomentWindow(moments=moments)
    
    after_time = moments[1].time
    before_time = moments[3].time
    
    filtered = window.get_moments(after=after_time, before=before_time)
    
    assert len(filtered) == 3
    assert filtered[0] == moments[1]
    assert filtered[-1] == moments[3]

def test_get_latest_and_day_frame(moments):
    """Tests getting the latest moment and its daily frame."""
    window = MomentWindow(moments=moments)
    
    latest_moment = window.get_latest()
    assert latest_moment == moments[-1]
    
    day_frame = window.get_day_frame()
    assert isinstance(day_frame, DailyFrame)
    
    # Check if the latest moment's time falls within the calculated frame
    frame_start = day_frame.get_floor().astimezone(EASTERN_TIMEZONE)
    frame_end = day_frame.get_ceiling().astimezone(EASTERN_TIMEZONE)
    latest_time_eastern = latest_moment.time.astimezone(EASTERN_TIMEZONE)
    
    assert frame_start <= latest_time_eastern < frame_end
