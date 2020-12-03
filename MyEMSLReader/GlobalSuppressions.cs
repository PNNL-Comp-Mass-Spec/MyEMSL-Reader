// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
[assembly: SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Keep for reference", Scope = "member", Target = "~M:MyEMSLReader.Downloader.DownloadAndExtractTarFile(System.Net.CookieContainer,System.Collections.Generic.List{MyEMSLReader.ArchivedFileInfo},System.Int64,System.Collections.Generic.IReadOnlyDictionary{System.Int64,System.String},System.IO.FileSystemInfo,MyEMSLReader.Downloader.DownloadLayout,System.String,System.Int32)~System.Boolean")]
