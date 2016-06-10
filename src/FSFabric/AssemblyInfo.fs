namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FSFabric")>]
[<assembly: AssemblyProductAttribute("FSFabric")>]
[<assembly: AssemblyDescriptionAttribute("A set of modules and functions for working with Service Fabric in F#.")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
    let [<Literal>] InformationalVersion = "1.0"
