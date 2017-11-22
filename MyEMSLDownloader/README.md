== Overview ==

This program downloads files from MyEMSL.  
You use the dataset name to specific the files to download 
(wildcards are supported).  Optionally specify a subfolder
or filename mask to filter the files to retrieve.  Use /Preview
to preview the files that will be downloaded.

== Details ==

Program syntax #1:
MyEMSLDownloader.exe DatasetName [SubFolderName] [/Files:FileMask] [/FileSplit]
                     [/O:OutputFolder] [/D] [/Preview] [/V] [/DisableCart] [/ForceCart] [/UseTest]

Program syntax #2:
MyEMSLDownloader.exe /Dataset:DatasetName [/SubDir:SubFolderName] [/Files:FileMask] [/FileSplit]
                     [/O:OutputFolder] [/D] [/Preview] [/V] [/DisableCart] [/ForceCart] [/UseTest]

Program syntax #3:
MyEMSLDownloader.exe /DataPkg:DataPackageID [/SubDir:SubFolderName] [/Files:FileMask] [/FileSplit]
                     [/O:OutputFolder] [/Preview] [/V] [/DisableCart] [/ForceCart] [/UseTest]

Program syntax #4:
MyEMSLDownloader.exe /FileList:FileInfoFile.txt [/O:OutputFolder]
                     [/Preview] [/V] [/DisableCart] [/ForceCart] [/UseTest]

Program syntax #5:
MyEMSLDownloader.exe /Test [/Preview] [/V] [/DisableCart] [/ForceCart]

To download files for a given dataset, enter the dataset name, plus optionally the SubFolder name
The names can be entered separated by spaces, or using /Dataset plus optionally /SubDir

Use /Files to filter for specific files, for example /Files:*.txt
Files will be downloaded to the folder with the .exe; override using /O
Use /FileSplit to indicate that /Files contains a list of filenames and/or file specs, separated by semicolons
For example, use /Files:analysis.baf;ser /FileSplit

Use /D to create a folder with the dataset name, then store the files within that folder

Use /DataPkg to retrieve files from a specific data package

Use /FileList to specify a file with a list of datasets and files to retrieve
The file must be a tab-delimited text file, with columns Dataset and File, and optionally with column SubDir
The file names in the File column are allowed to contain wildcards
When /FileList is used, /D is automatically enabled

Alternatively, use /Test to perform automatic tests using predefined dataset names

Use /Preview to view files that would be downloaded, but not actually download them
Use /V to enable verbose preview, showing extended details about each file

Use /DisableCart to disable use of the download cart mechanism for retrieving files that exist on tape but not on spinning disk
Use /ForceCart to force the use of the download cart; this option overrides /DisableCart

Use /UseTest to connect to test0.my.emsl.pnl.gov instead of my.emsl.pnl.gov

-------------------------------------------------------------------------------
Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013

E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/
-------------------------------------------------------------------------------

Licensed under the Apache License, Version 2.0; you may not use this file except 
in compliance with the License.  You may obtain a copy of the License at 
http://www.apache.org/licenses/LICENSE-2.0

Notice: This computer software was prepared by Battelle Memorial Institute, 
hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the 
Department of Energy (DOE).  All rights in the computer software are reserved 
by DOE on behalf of the United States Government and the Contractor as 
provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY 
WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS 
SOFTWARE.  This notice including this sentence must appear on any copies of 
this computer software.
