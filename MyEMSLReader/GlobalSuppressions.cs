// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
[assembly: SuppressMessage("General", "RCS1079:Throwing of new NotImplementedException.", Justification = "Might implement this in the future", Scope = "member", Target = "~M:MyEMSLReader.Downloader.DownloadFilesViaCart(System.Collections.Generic.Dictionary{System.Int64,MyEMSLReader.ArchivedFileInfo},System.Net.CookieContainer,System.Collections.Generic.Dictionary{System.Int64,System.String},System.IO.DirectoryInfo,MyEMSLReader.Downloader.DownloadLayout,System.Int64@)~System.Boolean")]
