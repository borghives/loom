
import time
from contextlib import nullcontext

import pytest

from loom.time.perftime import PerfTimer, sub_timed, timed


def test_perf_timer_basic():
    """Tests basic functionality of PerfTimer."""
    timer = PerfTimer(name="basic_test")
    with timer:
        time.sleep(0.01)
    
    assert timer.count == 1
    assert timer.total_time > 0.009
    assert timer.name == "basic_test"
    assert len(timer.child_timers) == 0


def test_perf_timer_nested():
    """Tests nested timing with sub_timer."""
    parent_timer = PerfTimer(name="parent")
    
    with parent_timer:
        time.sleep(0.01)
        child_timer = parent_timer.sub_timer("child")
        with child_timer:
            time.sleep(0.02)

    assert parent_timer.count == 1
    assert "child" in parent_timer.child_timers
    
    child_timer = parent_timer.child_timers["child"]
    assert child_timer.count == 1
    assert child_timer.depth == 1
    assert child_timer.total_time > 0.019
    assert parent_timer.total_time > child_timer.total_time


def test_timed_decorator_simple():
    """Tests the @timed decorator on a simple function."""
    
    @timed
    def timed_function():
        time.sleep(0.01)

    # Since the decorator creates its own timer, we can't directly inspect it
    # without more complex machinery (like patching).
    # For this test, we just ensure it runs without error.
    # A more complete test is done via nesting.
    timed_function()


def test_timed_decorator_with_args():
    """Tests the @timed decorator with arguments."""
    
    @timed(name="custom_name")
    def timed_function_with_name():
        pass

    # Just ensure it runs without error
    timed_function_with_name()


def test_timed_decorator_nested_injection():
    """Tests nested timing using decorator and ptimer injection."""
    
    parent_timer = PerfTimer(name="root")

    @timed(name="child_func")
    def inner_function(ptimer: PerfTimer):
        assert ptimer is not None
        assert ptimer.name == "child_func"
        with ptimer.sub_timer("grandchild"):
            time.sleep(0.01)
        time.sleep(0.01)

    # The decorator should use the passed-in ptimer as the parent
    inner_function(ptimer=parent_timer)

    assert "child_func" in parent_timer.child_timers
    child_timer = parent_timer.child_timers["child_func"]
    assert child_timer.count == 1
    assert "grandchild" in child_timer.child_timers
    grandchild_timer = child_timer.child_timers["grandchild"]
    assert grandchild_timer.count == 1
    assert grandchild_timer.total_time > 0.009


def test_sub_timed_helper():
    """Tests the sub_timed helper function."""
    parent = PerfTimer("parent")
    
    # With a valid timer
    with sub_timed(parent, "child") as child:
        assert isinstance(child, PerfTimer)
        assert child.name == "child"

    # With None
    with sub_timed(None, "child") as should_be_null:
        # The nullcontext() context manager yields None.
        assert should_be_null is None

def test_timer_str_output():
    """Tests the string representation of the timer."""
    timer = PerfTimer("test_str")
    with timer:
        with timer.sub_timer("child1"):
            time.sleep(0.01)
        with timer.sub_timer("child2"):
            time.sleep(0.02)

    output = str(timer)
    # print(output)

    assert "test_str" in output
    assert "ms" in output
    assert "times" in output
    assert "└── child2" in output  # Test for tree structure and sorting
    assert "└── child1" in output
    assert output.find("child2") < output.find("child1") # child2 took longer, should appear first
    assert "%" in output  # Percentages should be present for children

    timer_us = PerfTimer("test_us")
    with timer_us:
        time.sleep(0.00001)  # us range

    output_us = str(timer_us)
    assert "us" in output_us

    timer_s = PerfTimer("test_s")
    with timer_s:
        time.sleep(1.01)  # s range

    output_s = str(timer_s)
    assert "s" in output_s
    assert "avg" not in output_s  # avg only shows for count > 1

    # Test avg
    with timer_s:
        time.sleep(0.01)

    output_s_avg = str(timer_s)
    assert timer_s.count == 2
    assert "avg" in output_s_avg


def test_elapsed_property():
    """Tests the elapsed property."""
    timer = PerfTimer("elapsed_test")
    assert timer.elapsed == 0.0
    
    with timer:
        time.sleep(0.01)
        assert timer.elapsed > 0.009
    
    assert timer.elapsed == 0.0 # Should be 0 after stopping


def test_timed_decorator_async():
    """Tests the @timed decorator with an async function."""
    import asyncio

    parent_timer = PerfTimer(name="async_root")

    @timed(name="async_child")
    async def async_function(ptimer: PerfTimer):
        assert ptimer.name == "async_child"
        await asyncio.sleep(0.01)

    asyncio.run(async_function(ptimer=parent_timer))

    assert "async_child" in parent_timer.child_timers
    child_timer = parent_timer.child_timers["async_child"]
    assert child_timer.count == 1
    assert child_timer.total_time > 0.009


def test_to_dict_method():
    """Tests the to_dict method for structured data export."""
    root = PerfTimer(name="root")
    with root:
        with root.sub_timer("child1"):
            time.sleep(0.01)
        with root.sub_timer("child2"):
            time.sleep(0.02)

    result_dict = root.to_dict()

    assert result_dict["name"] == "root"
    assert result_dict["count"] == 1
    assert result_dict["total_time"] > 0.029
    assert len(result_dict["children"]) == 2

    child2_dict = result_dict["children"][0]
    child1_dict = result_dict["children"][1]

    # Check sorting (child2 took longer, should be first)
    assert child2_dict["name"] == "child2"
    assert child1_dict["name"] == "child1"

    assert child1_dict["count"] == 1
    assert child1_dict["total_time"] > 0.009
    assert len(child1_dict["children"]) == 0
