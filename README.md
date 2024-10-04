# Raft-Implementation

## About the project
- This repository contains a simple [RAFT](https://raft.github.io/raft.pdf) consensus algorithm with leader lease implemented as part of the [CSE530](https://techtree.iiitd.edu.in/viewDescription/filename?=CSE530) course offered at IIIT-D.
- Use the files node.py and client.py to run the RAFT algorithm locally.
- Use the files gcp-node.py and gcp-client.py to run the RAFT algorithm on Google Cloud Platform (or any other cloud instance).
- Team Members: Jaskaran Singh, Gautam Reddy

## Introduction
- **Raft** is a consensus algorithm designed to be easy to understand. Raft divides time into terms, and each term begins with an election to select a leader. The leader manages the replication of the log entries to the other servers and ensures a high degree of consistency.
- This implementation of RAFT also employs leader lease for optimized read only queries. Leases can be considered as tokens that are valid for a certain period of time, known as lease duration. A node can serve read and write requests from the client if and only if the node has this valid token/lease.
- For a more detailed explanation please refer the following [page](https://www.yugabyte.com/blog/low-latency-reads-in-geo-distributed-sql-with-raft-leader-leases/#:~:text=This%20is%20shown%20in%20the%20animation%20sequence%20below.).
<p align="center">
  <img src="https://miro.medium.com/v2/resize:fit:640/format:webp/1*bm1c0vS14qw1mU_zDpYxRw.gif" alt="RAFT election demonstration"/>
</p>

## Remote Procedure Calls (RPCs) 
- Raft servers communicate using remote procedure calls (RPCs), and the basic consensus algorithm requires only two types of RPCs. 
- **RequestVote** RPCs are initiated by candidates during elections.
- **AppendEntries** RPCs are initiated by leaders to replicate log entries and to provide a form of heartbeat.
- For this project, [**gRPC**](https://grpc.io) has been used to facilitate RPC functionality.
- The RPCs utilized in this project are detailed in the below figure (Note: The RPCs contain slight modifications to incorporate the leader lease functionality): 

<p align="center">
  <img src="images/RPCs.png" alt="RAFT election demonstration"/>
</p>

## References
- RAFT explanation articles: [PART-1](https://towardsdatascience.com/raft-algorithm-explained-a7c856529f40), [PART-2](https://towardsdatascience.com/raft-algorithm-explained-2-30db4790cdef)
- Low Latency reads using Leader Lease: [Yugabyte Blog](https://www.yugabyte.com/blog/low-latency-reads-in-geo-distributed-sql-with-raft-leader-leases/#:~:text=This%20is%20shown%20in%20the%20animation%20sequence%20below.)

## Acknowledgments
- Election animation GIF source: [Alexander Brooks's Medium article](https://medium.com/@alexander.jw.brooks/inside-raft-an-algorithm-for-consensus-in-asynchronous-distributed-systems-984f353c3feb)
- README.md: [Link](https://github.com/adityaahuja7/Raft-Implementation)
