APPNAME=container
SOURCES=container.cpp

CXXFLAGS=--std=c++11 -Wall -O0 -ggdb -I/opt/boost161/include
LDFLAGS=-L/opt/boost161/lib -pthread -lboost_system -lboost_chrono -o $(APPNAME)

default: all

clean:
	rm -f *.o
	rm -f $(APPNAME)
all:
	g++ $(CXXFLAGS) $(LDFLAGS) $(SOURCES)
