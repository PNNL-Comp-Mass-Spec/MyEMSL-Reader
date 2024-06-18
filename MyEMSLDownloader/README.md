# MyEMSL Downloader

This program downloads files from MyEMSL. It supports retrieving files
for either Datasets or Data Packages. Optionally specify a subdirectory
or filename mask to filter the files to retrieve. Use `/Preview`
to preview the files that will be downloaded.

## Mode 1 Syntax 

```
MyEMSLDownloader.exe DatasetNameOrID [SubdirectoryName] [/Files:FileMask] [/FileID:FileIDs] [/FileSplit]
 [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/UseTest]
```

## Mode 2 Syntax 

```
MyEMSLDownloader.exe /Dataset:DatasetName [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileID:FileIDs] [/FileSplit]
 [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/UseTest]
```

## Mode 3 Syntax 

```
MyEMSLDownloader.exe /DatasetID:DatasetID [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileID:FileIDs] [/FileSplit]
 [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/UseTest]
```

## Mode 4 Syntax 

```
MyEMSLDownloader.exe /DataPkg:DataPackageID [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileID:FileIDs] [/FileSplit]
 [/O:OutputDirectory] [/Preview] [/V] [/Trace] [/UseTest]
```

## Mode 5 Syntax 

```
MyEMSLDownloader.exe /FileList:FileInfoFile.txt [/O:OutputDirectory]
 [/Preview] [/V] [/Trace] [/UseTest]
```

## Mode 6 Syntax 

```
MyEMSLDownloader.exe /Test [/Preview] [/V] [/Trace]
```

## Command Line Arguments

To download files for a given dataset, enter the dataset name or dataset ID, plus optionally the subdirectory name
* Alternatively, use `/Dataset` or `/DatasetID` plus optionally `/SubDir`

Use `/Files` to filter for specific files, for example `/Files:*.txt`
* Files will be downloaded to the directory with the .exe; override using `/O`

Use `/FileSplit` to indicate that `/Files` contains a list of filenames and/or file specs, separated by semicolons
* For example, use `/Files:analysis.baf;ser /FileSplit`

Use `/D` to create a directory with the dataset name, then store the files within that directory

Use `/DataPkg` to retrieve files from a specific data package

Use `/FileList` to specify a file with a list of datasets and files to retrieve.
* The file must be a tab-delimited text file, with columns `Dataset` (or `DatasetID`) and `File`, and optionally with column `SubDir`
* The file names in the File column are allowed to contain wildcards
* When `/FileList` is used, `/D` is automatically enabled

Use `/FileID` or `/FileIDList`to specify one or more MyEMSL file IDs to download (as seen with `/V`)
* The files must be associated with the given dataset or data package (as specified by `/Dataset`, `/DatasetID`, or `/DataPkgID`)
* Supports a comma separated list of file IDs

Use `/IncludeAll` to show or download all versions of a file if MyEMSL has multiple versions of the same file (by file path)
* A suffix will be applied to files that have multiple versions
* If `/IncludeAll` is not used, only the newest version of a file is downloaded

Use `/Test` to perform automatic tests using predefined dataset names

Use `/Preview` to view files that would be downloaded, but not actually download them
* Use `/V` or `/Verbose` to enable verbose preview, showing extended details about each file

Use `/Trace` to display additional debug information

Use `/UseTest` to connect to test0.my.emsl.pnl.gov instead of my.emsl.pnl.gov

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov \
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics

## License

The MyEMSL Downloader is licensed under the 2-Clause BSD License; 
you may not use this program except in compliance with the License. You may obtain 
a copy of the License at https://opensource.org/licenses/BSD-2-Clause

Copyright 2018 Battelle Memorial Institute
