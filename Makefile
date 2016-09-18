APPNAME=container
TESTNAME=container_test
APPSOURCES=container.cpp
TESTSOURCES=container_test.cpp

CC=g++

CXXFLAGS=--std=c++11 -Wall -O3 -g0 -I/opt/boost161/include
LDFLAGS=-L/opt/boost161/lib -pthread -lboost_system -lboost_chrono

.PHONY: all test clean

default: clean all test

clean:
	rm -f *.o
	rm -f $(APPNAME)
	rm -f $(TESTNAME)

all: $(APPNAME)

test: $(TESTNAME)

$(APPNAME):
	g++ $(CXXFLAGS) $(LDFLAGS) $(APPSOURCES) -o $(APPNAME)

$(TESTNAME):
	g++ $(CXXFLAGS) $(LDFLAGS) -lboost_unit_test_framework $(TESTSOURCES) -o $(TESTNAME)
