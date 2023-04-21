
Compile the code using the provided makefile:

    make

To clear the directory (and remove .txt files):
   
    make clean

To run the coordinator:

    ./coord -p <coordinatorPort>

To run the server:

    ./tsd -i <coordinatorIP> -c <coordinatorPort> -p <portNum> -d <idNum> -t <master/slave>

To run the client  

    ./tsc -h <coordinatorIP> -p <coordinatorPort> -u <clientId>


