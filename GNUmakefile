COMMON_OBJECTS = md5.o reader.o unicast_sender.o file_writer.o distributor.o
UNCOMMON_OBJECTS = mcp.o mcpd.o

DEP_FILES = $(COMMON_OBJECTS:.o=.d) $(UNCOMMON_OBJECTS:.o=.d)

CXXFLAGS += -ggdb -Wall
#CXXFLAGS += -DNDEBUG

PROGRAMS = mcp mcpd

all: $(PROGRAMS)

mcp: mcp.o $(COMMON_OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

mcpd: mcpd.o $(COMMON_OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

sinclude $(DEP_FILES)

%.d:%.cpp
	@$(CXX) -MM $< >$@

clean:
	rm -f $(PROGRAMS) $(COMMON_OBJECTS) $(UNCOMMON_OBJECTS) $(DEP_FILES)

.PHONY: clean
