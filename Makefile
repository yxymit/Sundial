CC=g++
CFLAGS=-Wall -g -std=c++11

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./transport/ ./utils/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./transport -I./utils

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -O3 -g -ggdb
LDFLAGS = -Wall -L./libs -pthread -lrt -std=c++0x -O3 -ljemalloc
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp %.d
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb *.o */*.o *.d */*.d
