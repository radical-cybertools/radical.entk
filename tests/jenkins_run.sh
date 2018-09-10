# Preparatory
export RMQ_PORT=32773
export RMQ_HOSTNAME='one.radical-project.org'
export RADICAL_PILOT_DBURL="mongodb://entk:entk123@ds149742.mlab.com:49742/entk-0-7-5"
cp .coveragerc .coveragerc_bak
sed -i 's|VENV|'"$VENV"'|g' .coveragerc

# Run unit tests
coverage run -m pytest -vvv test_component/test_amgr.py
coverage run -m pytest -vvv test_component/test_modules.py
coverage run -m pytest -vvv test_component/test_pipeline.py
coverage run -m pytest -vvv test_component/test_rmgr_2.py
coverage run -m pytest -vvv test_component/test_rmgr.py
coverage run -m pytest -vvv test_component/test_stage.py
coverage run -m pytest -vvv test_component/test_task.py
coverage run -m pytest -vvv test_component/test_tmgr_2.py
coverage run -m pytest -vvv test_component/test_tmgr_rp_utils_2.py
coverage run -m pytest -vvv test_component/test_tmgr_rp_utils.py
coverage run -m pytest -vvv test_component/test_tmgr.py
coverage run -m pytest -vvv test_component/test_wfp.py
coverage run -m pytest -vvv test_integration/test_*
coverage run -m pytest -vvv test_issues/test_*
coverage run -m pytest -vvv test_utils/test_*

# Generate coverage report and upload to codecov
coverage combine
coverage xml
codecov
curl -s https://codecov.io/bash | bash

# Cleanup
rm .coveragerc
mv .coveragerc_bak .coveragerc
rm .re.session.* re.session.* rp.session.* test/ test.* -rf

