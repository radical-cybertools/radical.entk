import pytest
from radical.entk.utils import get_session_profile, get_session_description
from pprint import pprint
import radical.utils as ru

def test_get_session_profile():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017598.0002'
    src = './sample_data/profiler'
    profile, acc, hostmap = get_session_profile(sid=sid, src=src)

    assert profile == ru.read_json('./sample_data/profiler/expected_profile.json')['profile']
    assert isinstance(acc, float) 
    assert isinstance(hostmap,dict)

def test_get_session_description():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017598.0002'
    src = './sample_data/profiler'
    desc = get_session_description(sid=sid, src=src)

    assert desc == ru.read_json('./sample_data/profiler/expected_desc.json')