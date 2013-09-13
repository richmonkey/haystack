Introduction
---------
A simple distribute filesystem used for saving a billion of small file.
Haystack append small file into a very large file for reduce the inode's count.
You can find more detailed design at doc/Beaver.pdf

Run
---------
track 
python2.7 haystack_track.py xxx.conf

storage
python2.7 haystack_storage.py xxx.conf

Client
---------
Upload:
  - request writable storage node from track node
  - post file to the writable storage node

Download:
  - request readable storage node by fileid from track node
  - get file from the readable storage node
