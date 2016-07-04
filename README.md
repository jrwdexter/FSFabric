# FSFabric
A collection of utilities for working with Service Fabric in F#.

## Modules

Currently, only one module exists within FSFabric: FabricStore.

### FabricStore

FabricStore lifts operating on IReliableDictionarys into the AsyncSeq space. Evaluations are lazy evaluated, and when possible, use native calls (eg. `AddOrUpdateAsync()`). However, the majority of applications require enumerating the dictionary, so caution is advised.

FabricStore is very simple to use:

**Querying a Fabric Store, then updating it**
We will:
* Query a dictionary for all items that start with 'x'
* Update each of those values to have a property "Age" that is increased by 1.
* Update each of these items
* Commit these to the store
* Return the first such updated item

Note: 

* `>>=` corresponds to Async.bind

This operator is included under FSFabric.Async.Operators, if they have not been added to your code already.

```f#
FabricStore.initDict<string, Person> stateManager "MyDictionaryName"
>>= FabricStore.filter (fun (k, _) -> k.StartsWith("x"))
>>= FabricStore.map (fun (k, v) -> k, { v with Age = v.Age + 1 })
>>= FabricStore.update "MyDictionaryName"
>>= FabricStore.commit
>>= FabricStore.tryFirst
```

A FabricStore has three stages:

* Closed: Has no open transaction.
* Open: Has an open transaction.
* OutsideOpenedStore: Transaction was provided with `useTx`, allowing transactions to be shared between FabricStores.

The above case automatically creates a transaction, and then closes it with the `commit` call. `commit` and `close` both end a transaction that was opened automatically, while neither will close a transaction provided with `useTx`. External transactions must be closed manually.
