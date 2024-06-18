# Pacifica

The Pacifica DLL is used to interact with MyEMSL, 
including both pushing data into MyEMSL and extracting data from MyEMSL.

## Dependencies

| Namespace              | Dependency                       | Source |
|------------------------|----------------------------------|--------|
| Pacifica.Json          | NewtonSoft.Json                  | NuGet  |
|                        |                                  |        |
| Pacifica.Core          | Pacifica.Json (namespace)        | NuGet  |
| Pacifica.Core          | PRISM-Library                    | NuGet  |
|                        |                                  |        |
| Pacifica.DataUpload    | Pacifica.Json (namespace)        | Local  |
| Pacifica.DataUpload    | Pacifica.Core (namespace)        | Local  |
| Pacifica.DataUpload    | PRISM-Library                    | NuGet  |
| Pacifica.DataUpload    | SharpZipLib                      | NuGet  |
|                        |                                  |        |
| Pacifica.DMSDataUpload | Pacifica.Json (namespace)        | Local  |
| Pacifica.DMSDataUpload | Pacifica.Core (namespace)        | Local  |
| Pacifica.DMSDataUpload | Pacifica.DataUpload (namespace)  | Local  |
| Pacifica.DMSDataUpload | PRISM-Library                    | NuGet  |
| Pacifica.DMSDataUpload | PRISM-DatabaseUtils              | NuGet  |

## Contacts

Written by Kevin Fox, Ken Auberry, and Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov \
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics

## License

Licensed under the 2-Clause BSD License; you may not use this program 
except in compliance with the License. You may obtain a copy of the License at 
https://opensource.org/licenses/BSD-2-Clause

Copyright 2018 Battelle Memorial Institute
