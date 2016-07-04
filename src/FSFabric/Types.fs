module FSFabric.Types

open System
open FSharp.Control
open Microsoft.ServiceFabric.Data
open Microsoft.ServiceFabric.Data.Collections

type StoreValues<'TKey,'T when 'TKey :> IComparable<'TKey> and 'TKey :> IEquatable<'TKey>> =
    | Empty
    | Dictionary of IReliableDictionary<'TKey, 'T>
    | Values of AsyncSeq<'TKey * 'T>

type ValueStore<'TKey, 'T when 'TKey :> IComparable<'TKey> and 'TKey :> IEquatable<'TKey>> =
    | EmptyStore of IReliableStateManager
    | DictionaryStore of IReliableStateManager * IReliableDictionary<'TKey, 'T>
    | ResultStore of IReliableStateManager * AsyncSeq<'TKey * 'T>

type FabricStore<'TKey, 'T when 'TKey :> IComparable<'TKey> and 'TKey :> IEquatable<'TKey>> =
    | ClosedStore of ValueStore<'TKey, 'T>
    | OpenedStore of ITransaction * ValueStore<'TKey, 'T>
    | OutsideOpenedStore of ITransaction * ValueStore<'TKey, 'T> // A store with a transaction that was provided out of scope

