// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
[assembly: SuppressMessage("Performance", "RCS1197:Optimize StringBuilder.Append/AppendLine call.", Justification = "Optimization not necessary", Scope = "module")]
[assembly: SuppressMessage("Redundancy", "RCS1163:Unused parameter.", Justification = "Parameter on obsolete method", Scope = "member", Target = "~M:Pacifica.Core.Utilities.ValidateRemoteCertificate(System.Object,System.Security.Cryptography.X509Certificates.X509Certificate,System.Security.Cryptography.X509Certificates.X509Chain,System.Net.Security.SslPolicyErrors,System.String@)~System.Boolean")]
