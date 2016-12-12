OS:=$(shell uname)

MPICXX = mpic++
CXXFLAGS = -I${BOOST_INC} -O3 -std=c++0x

ifeq ($(OS), Darwin)
	LDFLAGS = ${BOOST_LIB} -lboost_mpi-mt -lboost_filesystem-mt -lboost_system-mt
else
	#LDFLAGS = -L${BOOST_LIB} -lboost_mpi -lboost_filesystem -lboost_system
	CXXFLAGS = -O3 -std=c++0x
	LDFLAGS = -lboost_mpi -lboost_filesystem -lboost_system -lboost_serialization
endif

all: specfem_mockup

specfem_mockup: specfem_mockup.cpp
	mkdir -p bin
	$(MPICXX) -o bin/$@ $^ ${CXXFLAGS} ${LDFLAGS}

clean: 
	rm bin/specfem_mockup
