CC=g++

HEADERS=container.h

APPNAME=fs_dump
APPSOURCES=$(APPNAME).cpp
APPOBJECTS=$(APPNAME).o

TESTNAME=container_test
TESTSOURCES=container_test.cpp
TESTOBJECTS=container_test.o

CXXFLAGS=-std=c++11 -c -DBOOST_LOG_DYN_LINK -Wall -O3 -g0 -isystem/opt/boost161/include
LDFLAGS_COMMON=-L/opt/boost161/lib -pthread -lboost_system -lboost_chrono
LDFLAGS_APP=$(LDFLAGS_COMMON) -lboost_filesystem -lboost_log
LDFLAGS_TEST=$(LDFLAGS_COMMON) -lboost_unit_test_framework


default: all

clean:
	rm -fr *.o $(APPNAME) $(TESTNAME)

all: $(APPSOURCES) $(TESTSOURCES) $(APPNAME) $(TESTNAME)

$(APPNAME): $(APPOBJECTS)
	$(CC) $(APPOBJECTS) $(LDFLAGS_APP) -o $@

$(TESTNAME): $(TESTOBJECTS)
	$(CC) $(TESTOBJECTS) $(LDFLAGS_TEST) -o $@

%.o: %.cpp $(HEADERS)
	$(CC) $(CXXFLAGS) $< -o $@
