
.PHONY: clean test doc

clean:
	-rm -rf build/ temp/ MANIFEST dist/ src/*.egg-info pylint.out *.egg *.dat *.sum
	-rm -rf doc/_build/*
	find . -name \*.pyc -exec rm -f {} \;
	find . -name \*.egg-info -exec rm -rf {} \;
    
test:
	python setup.py test

doc:
	(cd ./doc && make html)
