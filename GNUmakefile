MCP_OBJECTS = mcp.o \
md5.o \
reader.o \
file_reader.o \
unicast_sender.o \
multicast_sender.o \
distributor.o \
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
file_writer.o \
path.o \
distributor.o \
connection.o

DEP_FILES = $(MCP_OBJECTS:.o=.d) $(MCPD_OBJECTS:.o=.d)


#CXXFLAGS += -ggdb -Wall
CXXFLAGS += -DNDEBUG -O2

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
