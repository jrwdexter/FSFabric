namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FSFabric")>]
[<assembly: AssemblyProductAttribute("FSFabric")>]
[<assembly: AssemblyDescriptionAttribute("A set of modules and functions for working with Service Fabric in F#.")>]
[<assembly: AssemblyVersionAttribute("0.0.5")>]
[<assembly: AssemblyFileVersionAttribute("0.0.5")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.5"
    let [<Literal>] InformationalVersion = "0.0.5"
