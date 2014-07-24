
.PHONY: clean test doc

clean:
	-rm -rf build/ temp/ MANIFEST dist/ src/*.egg-info pylint.out *.egg
	-rm -rf doc/_build/*
	find . -name \*.pyc -exec rm -f {} \;
	find . -name \*.egg-info -exec rm -rf {} \;
    
test:
	python setup.py test

doc:
	virtualenv /tmp/radical.ensemblemd.docgen
	. /tmp/radical.ensemblemd.docgen/bin/activate
	easy_install -U sphinx
	(cd ./doc && make html)
