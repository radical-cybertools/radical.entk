import pytest
from radical.entk.utils import get_session_profile, get_session_description
from pprint import pprint
from radical.entk.exceptions import *
import radical.utils as ru
import os

def test_get_session_profile():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017598.0002'
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data/profiler'%curdir
    profile, acc, hostmap = get_session_profile(sid=sid, src=src)

    # ip_prof = ru.read_json('%s/expected_profile.json'%src)
    for item in profile:
        assert len(item) == 8
    assert isinstance(acc, float) 
    assert isinstance(hostmap,dict)

def test_get_session_description():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017598.0002'
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data/profiler'%curdir
    desc = get_session_description(sid=sid, src=src)

    assert desc == ru.read_json('%s/expected_desc.json'%src)