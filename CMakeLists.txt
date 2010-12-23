PROJECT( mcp )

  SET(mcp_sources
    mcp.cpp
    file_reader.cpp)


  SET(mcpd_sources
    mcpd.cpp
    file_writer.cpp
    multicast_error_queue.cpp
    multicast_receiver.cpp
    multicast_recv_queue.cpp
    multicast_send_queue.cpp
    multicast_sender.cpp
    path.cpp
    unicast_receiver.cpp)

  SET(mcp_common_sources
    connection.cpp
    distributor.cpp
    md5.cpp
    reader.cpp
    multicast_send_queue.cpp
    multicast_sender.cpp
    unicast_sender.cpp)

  #ADD_DEFINITIONS(-Wall -ggdb -O2 -DNDEBUG)
  ADD_DEFINITIONS(-Wall -ggdb -O2 -DNDEBUG -DUSE_MULTICAST)

  ADD_LIBRARY(mcp_common STATIC ${mcp_common_sources})
  LINK_LIBRARIES(mcp_common pthread)

  ADD_EXECUTABLE(mcp ${mcp_sources})
  ADD_EXECUTABLE(mcpd ${mcpd_sources})

  INSTALL(
    TARGETS mcp mcpd
    DESTINATION bin)

