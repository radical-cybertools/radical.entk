from radical.entk import AppManager
import pytest
from radical.entk.exceptions import *

def test_attribute_types():

    appman = AppManager()
    assert type(appman.name) == str

def test_assign_workload_exception():

    appman = AppManager()
    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:

        with pytest.raises(TypeError):
            appman.assign_workload(data)
