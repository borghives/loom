from unittest.mock import patch, MagicMock
from loom.info.directive import LoadDirective

def test_load_directive_count():
    """Tests the count method of the LoadDirective."""
    
    # Create a mock cursor that simulates the result of the aggregation
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = iter([{'count': 5}])

    # Patch the exec_aggregate method to return the mock cursor
    with patch.object(LoadDirective, 'exec_aggregate', return_value=mock_cursor):
        directive = LoadDirective(MagicMock())
        count = directive.count()
        
        # Assert that the count method returns the correct value
        assert count == 5

def test_load_directive_count_no_result():
    """Tests the count method of the LoadDirective when there are no results."""
    
    # Create a mock cursor that simulates an empty result
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = iter([])

    # Patch the exec_aggregate method to return the mock cursor
    with patch.object(LoadDirective, 'exec_aggregate', return_value=mock_cursor):
        directive = LoadDirective(MagicMock())
        count = directive.count()
        
        # Assert that the count method returns 0 when there are no results
        assert count == 0