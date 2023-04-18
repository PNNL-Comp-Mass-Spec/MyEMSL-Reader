// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Performance", "RCS1197:Optimize StringBuilder.Append/AppendLine call.", Justification = "Optimization not necessary", Scope = "module")]
[assembly: SuppressMessage("Roslynator", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required>", Scope = "member", Target = "~M:Pacifica.DataUpload.TarStreamUploader.SendFileListToIngester(Pacifica.Core.Configuration,System.String,System.String,System.Collections.Generic.SortedDictionary{System.String,Pacifica.Core.FileInfoObject},System.String,Pacifica.DataUpload.TarStreamUploader.UploadDebugMode)~System.String")]
[assembly: SuppressMessage("Roslynator", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required>", Scope = "member", Target = "~M:Pacifica.DMSDataUpload.MyEMSLUploader.Container_ProgressEvent(System.String,System.Single)")]
[assembly: SuppressMessage("Roslynator", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not required>", Scope = "member", Target = "~M:Pacifica.DMSDataUpload.MyEMSLUploader.MyEMSLUpload_StatusUpdate(System.Object,Pacifica.Core.StatusEventArgs)")]
