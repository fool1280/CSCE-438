cmake_minimum_required (VERSION 3.16)
project (chat_room_service VERSION 1.0)

find_package (glog 0.6.0 REQUIRED)

add_executable (client crc.cpp)
add_executable (server crsd.cpp)
target_link_libraries (client glog::glog)
target_link_libraries (server glog::glog)
