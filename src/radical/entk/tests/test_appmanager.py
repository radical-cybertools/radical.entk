from radical.entk import AppManager
import pytest
from radical.entk.exceptions import *

def test_attribute_types():

    """
    **Purpose**: Test all AppManager attribute types exposed to the user
    """

    appman = AppManager()
    assert type(appman.name) == str

    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:

        with pytest.raises(TypeError):
            appman.resource_manager = data


def test_assign_workload_exception():

    """
    **Purpose**: Test workload assignment and validation
    """

    appman = AppManager()
    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:

        with pytest.raises(Error):
            appman.assign_workflow(data)
