Change Log
==========
Version 1.0.0 *(2018-05-11)*
----------------------------
* Initial Commit

Version 1.0.1 *(2018-05-23)*
----------------------------
* Refactor and better usage of Rx

Version 1.0.2 *(2018-05-25)*
----------------------------
* Rx 2.1.14 and update logic to support change introduce in Rx 2.1.1 where mapper can no longer
return a null. 

Version 1.0.3 *(2018-05-25)*
----------------------------
* New AdaptableStore and StoreAdapter classes that can be used to persist to any storage layer such as rest api
* .gitignore udpate to exclude `out` directories
* Refactor `objects` package to `storable` package
* Google Java Format
* NullAway
* ErrorProne

Version 1.0.4 *(2018-06-14)*
----------------------------
* AdaptableStorage
* Update FileStorageUnit to extend AdaptableStorageUnit