
# Compile
Compile the code using the provided makefile:

    make

To clear the directory (and remove .txt files):
   
    make clean

# Run manually

To run the coordinator:

    ./coord -p <coordinatorPort>

To run the server:

    ./tsd -i <coordinatorIP> -c <coordinatorPort> -p <portNum> -d <idNum> -t <master/slave>

To run the client  

    ./tsc -h <coordinatorIP> -p <coordinatorPort> -u <clientId>

# Script
Using script:

    chmod +x startup.sh
    chmod +x end.sh

Run:

    ./startup.sh

Kill:

    ./end.sh
