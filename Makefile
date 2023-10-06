
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: kvstore.o correctness.o skiplist.o cache.o
persistence: kvstore.o persistence.o skiplist.o cache.o

clean:
	-rm -f correctness persistence *.o
