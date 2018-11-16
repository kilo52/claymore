#### 1.3.0
* Added remove capabilities to the *ConfigurationFile* class. Both Sections as well as individual entries can now be removed if desired
* Fixed DataFrame deserialization error when trying to read a *.df* file that represents an uninitialized DataFrame, i.e. no columns defined
* Added package-info documentation to all packages
* Fixed minor code formatting issues

#### 1.1.0
* Renamed *findAll()* methods to *filter()* in the DataFrame API. All *findAll()* methods are now deprecated and will be removed in a future release
* All *filter()* methods now return an empty DataFrame instead of null when there are no matches
* Return type of public methods in CSVFileReader and CSVFileWriter changed to allow method chaining

#### 1.0.0 
* Final version for open source release

