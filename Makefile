CC=g++
PROTOC=protoc
CPPF = `pkg-config --cflags protobuf grpc`

.SUFFIXES: .o .cpp .h 

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./grpc/ ./grpc_system/ ./utils/
PROTOS_PATH = protos
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

#CFLAGS=-Wall -g -std=c++0x
CFLAGS=-Wall -g -std=c++11
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./grpc -I./grpc_system -I./utils
CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -O0
#OTHERLAGS = `pkg-config --cflags protobuf grpc`

LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++0x -O0 -ljemalloc
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl

#CCS = $(wildcard *.cc)
CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)
#CCOBJS = $(CCS:.cc=.o)

vpath %.proto $(PROTOS_PATH)

all : rundb

rundb : $(OBJS) 
	$(CC) -no-pie -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp 
	$(CC) $(CPPF) -c $(CFLAGS) -o $@ $<

#%.grpc.pb.cc: %.proto
#	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<
#%.pb.cc: %.proto
#	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS) 