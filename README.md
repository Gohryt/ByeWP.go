# ByeWP.go
A simple tool for migration from WordPress.

Usage:  
Rewrite configuration jsons (old.json and new.json), paste your databases connection information (old is wordpress database).  
Build and start with (or without) these flags:  
make - program will make legacy part of new db;  
ask - program will ask you with suspicious cases;  
postsFrom - start migration from post with this id;  
postsFromEnd - start posts migration from last id in a new database.  
usersFrom - start migration from user with this id;  
usersFromEnd - start users migration from last id in a new database.
