# Distracted database
A really simple database that is distracted between multiple servers written in Java.

Project created at Polish Japanese Acadamy of Information Technology at Warsaw, 2024

## What is this

Database in this project is a network of singular server processes (nodes) written in Java.

Every process has in memory only one record with key and value. Both of these are integers.\
Network is created incrementally - to every node you can attach as many nodes as it is needed. It does not mean that every server has to be connected with another - servers are connected only with these that were given during startup and those attached later (during startups of other servers).\
Clients of the database can send requests to any server in the network. Based on the request server can reply with the response right away, or it may need to forward this request to other servers.

## Running server
```java DatabaseNode -tcpport <TCP port> -record <key>:<value> [-connect <address>:<port>]```

- `-tcpport <TCP port>` - declares TCP port at which server will be listening requests from clients and other servers
- `-record <key>:<value>` - declares record that will be stored by server, key and value do not have to be unique
- `-connect <address>:<port>` - optional argument that declares IP address to other running server; you can add many servers by using this argument with every IP address

Examples: 

```java DatabaseNode -tcpport 9001 -record 1:10```\
```java DatabaseNode -tcpport 9004 -record 4:40 -connect localhost:9003 -connect localhost:9001```

## Running client

**IMPORTANT**: Client process **is not written by me!**

`java DatabaseClient -gateway <address>:<port> -operation <operation with parameters>`
- `-gateway <address>:<port>` - optional argument that declares IP address to other running server; you can add many servers by using this argument with every IP address
- `-operation <operation with parameters>` - operation that will be processed by server

### Available operations

- `set-value <key>:<value>` - sets new value to the given key
- `get-value <key>` - returns value under given key
- `find-key <key>` - returns IP address of node that stores record with given key
- `get-max` - returns record with the highest value
- `get-min` - returns record with the lowest value
- `new-record <key>:<value>` - overrides the stored record with the given one
- `terminate` - disconnects server from the network and shuts it down

Examples:

```java DatabaseClient -gateway localhost:9002 -operation set-value 1:150```\
```java DatabaseClient -gateway localhost:9002 -operation get-min```
