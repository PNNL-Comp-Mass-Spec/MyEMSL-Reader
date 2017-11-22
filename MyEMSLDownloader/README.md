# MyEMSL Downloader

This program downloads files from MyEMSL.  It supports retrieving files
for either Datasets or Data Packages.  Optionally specify a subfolder
or filename mask to filter the files to retrieve.  Use /Preview
to preview the files that will be downloaded.

## Mode 1 Syntax 

```
MyEMSLDownloader.exe DatasetName [SubFolderName] [/Files:FileMask] [/FileSplit]
                     [/O:OutputFolder] [/D] [/Preview] [/V] 
                     [/Trace] [/DisableCart] [/ForceCart] [/UseTest]
```

## Mode 2 Syntax 

```
MyEMSLDownloader.exe /Dataset:DatasetName [/SubDir:SubFolderName] [/Files:FileMask] [/FileSplit]
                     [/O:OutputFolder] [/D] [/Preview] [/V] 
                     [/Trace] [/DisableCart] [/ForceCart] [/UseTest]
```

## Mode 3 Syntax 

```
MyEMSLDownloader.exe /DataPkg:DataPackageID [/SubDir:SubFolderName] [/Files:FileMask] [/FileSplit]
                     [/O:OutputFolder] [/Preview] [/V] 
                     [/Trace] [/DisableCart] [/ForceCart] [/UseTest]
```

## Mode 4 Syntax 

```
MyEMSLDownloader.exe /FileList:FileInfoFile.txt [/O:OutputFolder]
                     [/Preview] [/V] [/DisableCart] [/ForceCart] [/UseTest]
```

## Mode 5 Syntax 

```
MyEMSLDownloader.exe /FileID:1234 [/Preview] [/V] [/Trace]
```

## Mode 6 Syntax 

```
MyEMSLDownloader.exe /Test [/Preview] [/V] [/Trace] [/DisableCart] [/ForceCart]
```

## Command Line Arguments

To download files for a given dataset, enter the dataset name, plus optionally the SubFolder name
The names can be entered separated by spaces, or using `/Dataset` plus optionally `/SubDir`

Use `/Files` to filter for specific files, for example `/Files:*.txt`
Files will be downloaded to the folder with the .exe; override using `/O`

Use `/FileSplit` to indicate that `/Files` contains a list of filenames and/or file specs, separated by semicolons
For example, use `/Files:analysis.baf;ser /FileSplit`

Use `/D` to create a folder with the dataset name, then store the files within that folder

Use `/DataPkg` to retrieve files from a specific data package

Use `/FileList` to specify a file with a list of datasets and files to retrieve
The file must be a tab-delimited text file, with columns Dataset and File, and optionally with column SubDir
The file names in the File column are allowed to contain wildcards
When `/FileList` is used, `/D` is automatically enabled

Use `/FileId` to specify the MyEMSL ID of a file to download (as seen with `/V`)
This mode does not use Simple Search to find files and can thus be used to retrieve a file that Simple Search does not find. Provide a comma separated
 list to retrieve multiple files.

Alternatively, use `/Test` to perform automatic tests using predefined dataset names

Use `/Preview` to view files that would be downloaded, but not actually download them
Use `/V` to enable verbose preview, showing extended details about each file

Use `/Trace` to display additional debug information

Use `/DisableCart` to disable use of the download cart mechanism for retrieving files that exist on tape but not on spinning disk
Use `/ForceCart` to force the use of the download cart; this option overrides `/DisableCart`

Use `/UseTest` to connect to test0.my.emsl.pnl.gov instead of my.emsl.pnl.gov

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: proteomics@pnnl.gov \
Website: https://panomics.pnl.gov/ or https://omics.pnl.gov

## License

The MyEMSL Downloader is licensed under the Apache License, Version 2.0; 
you may not use this file except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/Apache-2.0
