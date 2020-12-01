# Pacifica

The Pacifica DLLs are used to interact with MyEMSL, 
including both pushing data into MyEMSL and extracting data from MyEMSL.

## Dependencies

| DLL                    | Dependency      | Source |
|------------------------|-----------------|--------|
| Pacifica.Core          | jayrock-json    | NuGet  |
| Pacifica.Core          | PRISM-Library   | NuGet  |
|                        |                 |        |
| Pacifica.Upload        | Pacifica.Core   | Local  |
| Pacifica.Upload        | PRISM-Library   | NuGet  |
| Pacifica.Upload        | SharpZipLib     | NuGet  |
|                        |                 |        |
| Pacifica.DMS_Metadata  | jayrock-json    | NuGet  |
| Pacifica.DMS_Metadata  | Pacifica.Core   | Local  |
| Pacifica.DMS_Metadata  | Pacifica.Upload | Local  |
| Pacifica.DMS_Metadata  | PRISM-Library   | NuGet  |

	
## Contacts

Written by Kevin Fox, Ken Auberry, and Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov \
Website: https://panomics.pnl.gov/ or https://omics.pnl.gov

## License

Licensed under the 2-Clause BSD License; you may not use this file 
except in compliance with the License. You may obtain a copy of the License at 
https://opensource.org/licenses/BSD-2-Clause

Copyright 2018 Battelle Memorial Institute
