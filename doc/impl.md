Inroduction
---------
Haystack has two type of nodes, tracker and storage.  
One tracker node connects with multiple storage nodes.  

Tracker
---------
Tracker node manages all storage nodes information in memory.
It exports two functions to client, request\_readable\_node and request\_writable\_node.
Normally two tracker nodes are deployed in production environment. Keepalived service is used to make one of them the standby node to guard against Single Point of Failure(SPOF) 

Storage
---------
Storage nodes are managered in groups.
The nodes in the same group has the same copies of files.  
One group has one master for write and read, other slave nodes for read and replication only.  
Storage exports two function upload, download.  
Upload:
   - get an auto-incremented fileno
   - append uploaded file content into the larger file
   - send fileid(groupid+fileno) to client
   - synchronize the file content to other slaves
   - report the new fileno to tracker node

Download:
   - find the file's offset and size in Google Sparse hash
   - send file content to client

Protocol
---------
Http.  
client with tracker, clien with storage, storage with tracker, storage with storage communicates with each other using http protocol.
 
Storage->Tracker
---------
Storage reports its infomation to tracker.  
The infomation contains last fileno, groupid, listenip, listenport, disk available size, write priority, read priority.

Storage->Storage
---------
Replication steps:
 - slave node connects to master node of the group at startup
 - slave node posts its last fileno to master
 - master node sends all the files after the slave's last fileno
 - if socket is disconnected, goto first step


Tracker Interface
=========

storage report
---------
**method:** POST  
**path:**/report  
**body:**
```js
{
"last_fileno":int,
"groupid":int,
"listenip":string,
"listenport":int,
"master":bool,
"disk\_available\_size":int
}
```
**response:**none  

request writable node
---------
**method:**GET  
**path:**/request\_writable\_node  
**body:**none  
**response:**
```js
{
"ip":string,
"port":int,
}
```

request readable node
--------
**method:**GET  
**path:**/request\_readable\_node?fileno=xxx  
**body:**none  
**response:**
```js
{
"ip":string,
"port":int,
}
```

Storage Interface
=========
client uploads file to storage
---------
**method:**POST  
**path:**/upload  
**body:**file binary content  
**response:**
```js
{
"fileid":int
}
```

client downloads files from storage
--------
**method:**GET  
**path:**/download?fileid=xxx  
**body:**none  
**response:**file binary content

master storage replicates files to slave nodes
--------
**method:**POST  
**path:**/sync_upload  
**body:**file binary content  
**response:**none

slave send sync request to master
--------
**method:**POST
**path:**/sync
**body:**
```js
{
"last_fileno":int
}
```
**response:**200


