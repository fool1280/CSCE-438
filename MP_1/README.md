### To build the code:

cmake . -B build

cd build

make

### To run the client:

GLOG_log_dir=. ./client 127.0.0.1 8080

### To run the server:

// Server runs on port 8080

GLOG_log_dir=. ./server
