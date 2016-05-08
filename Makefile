CC = gcc
RANLIB = ranlib

LIBSRC = MapReduceFramework.cpp Search.h Search.cpp

LIBOBJ = $(LIBSRC:.cpp=.o)

INCS = -I.
CFLAGS = -Wextra -Wall -std=c++11 -g $(INCS) 
LOADLIBES = -L./ 

LIB = MapReduceFramework.a
TARGETS = $(LIB) tar

TAR = tar
TARFLAGS = -cvf
TARNAME = ex3.tar
TARSRCS = $(LIBSRC) Makefile README

all: MapReduceFramework.o Search.cpp
	ar rcs $(LIB) MapReduceFramework.o
	ranlib $(LIB)
	g++ -Wextra -Wall -std=c++11 -pthread -g $(INCS) Search.cpp $(LIB) -o Search

search: $(LIB) Search.cpp
	g++ -Wextra -Wall -std=c++11 -pthread -g $(INCS) Search.cpp $(LIB) -o Search

MapReduceFramework.o: MapReduceFramework.cpp
	$(CC) $(CFLAGS) -c MapReduceFramework.cpp -o MapReduceFramework.o

$(LIB): MapReduceFramework.o
	ar rcs $(LIB) MapReduceFramework.o
	ranlib $(LIB)

clean:
	rm *.a *.o *.tar

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
	

