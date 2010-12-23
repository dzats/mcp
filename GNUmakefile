MCP_OBJECTS = mcp.o \
md5.o \
reader.o \
file_reader.o \
unicast_sender.o \
multicast_sender.o \
distributor.o \
errors.o \
connection.o \
multicast_send_queue.o
MCPD_OBJECTS = mcpd.o \
md5.o \
reader.o \
unicast_receiver.o \
unicast_sender.o \
multicast_sender.o \
multicast_send_queue.o \
multicast_receiver.o \
multicast_recv_queue.o \
multicast_error_queue.o \
file_writer.o \
path.o \
distributor.o \
errors.o \
connection.o

DEP_FILES = $(MCP_OBJECTS:.o=.d) $(MCPD_OBJECTS:.o=.d)

CXXFLAGS += -ggdb -Wall -O2 -DNDEBUG -DUSE_MULTICAST 
#CXXFLAGS += -ggdb -Wall -O2 -DUSE_MULTICAST #-DUSE_EQUATION_BASED_CONGESTION_CONTROL
#CXXFLAGS += -ggdb -Wall -O2 -DUSE_MULTICAST #-DUSE_EQUATION_BASED_CONGESTION_CONTROL

PROGRAMS = mcp mcpd

all: $(PROGRAMS)

mcp: $(MCP_OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

mcpd: $(MCPD_OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

sinclude $(DEP_FILES)

%.d:%.cpp
	@$(CXX) -MM $< >$@

clean:
	rm -f $(PROGRAMS) $(MCP_OBJECTS) $(MCPD_OBJECTS) $(DEP_FILES)

.PHONY: clean
