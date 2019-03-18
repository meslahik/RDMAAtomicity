# RDMAAtomicity
Compile:\
Let's say PathToRDMAAtomicity is the folder containing the RDMAAtomicity project files.

    cd PathToRDMAAtomicity
    mvn compile
    mvn dependency:copy-dependencies

Run:  
Assuming that your server is going to run on machine with IP 192.168.4.1. Then, try these steps:

Run Server:

    java -cp PathToRDMAAtomicity/target/classes:PathToRDMAAtomicity/target/dependency/* ch.usi.dslab.mojtaba.rdma.atomicity.Server -a 192.168.4.1
    
Run Client to write on remote memory:

    java -cp PathToRDMAAtomicity/target/classes:PathToRDMAAtomicity/target/dependency/* ch.usi.dslab.mojtaba.rdma.atomicity.ClientWrite -a 192.168.4.1
Run Client to read on remote memory:

    java -cp PathToRDMAAtomicity/target/classes:PathToRDMAAtomicity/target/dependency/* ch.usi.dslab.mojtaba.rdma.atomicity.ClientRead -a 192.168.4.1
    
    
The idea is to read the data written to by a remote machine and test the atomicity of reads locally and remotely.
The data written is a array of 100 bytes. The first client writes this array to the server's memory.
The server reads its local memory. A second client also reads the remote memory on the server.
If there is any mismatch between the values, the machine that is reading the array will report the difference meaning that the read is not atomic.
