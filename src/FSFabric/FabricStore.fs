namespace FSFabric

open System
open System.Threading
open Microsoft.ServiceFabric.Data
open Microsoft.ServiceFabric.Data.Collections
open FSharp.Control

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

module FabricStore =
    // Lifting value store
    let private mapValueMethod f store =
        match store with
        | ClosedStore vs ->
            ClosedStore (f vs)
        | OpenedStore (tx, vs) ->
            OpenedStore (tx, f vs)
        | OutsideOpenedStore (tx, vs) ->
            OutsideOpenedStore (tx, f vs)

    let private mapValueMethodAsync f store =
        async {
            match store with
            | ClosedStore vs ->
                let! result = f vs
                return ClosedStore (result)
            | OpenedStore (tx, vs) ->
                let! result = f vs
                return OpenedStore (tx, result)
            | OutsideOpenedStore (tx, vs) ->
                let! result = f vs
                return OutsideOpenedStore (tx, result)
        }

    let private doValueMethod f store =
        match store with
        | ClosedStore vs -> f vs
        | OpenedStore (_, vs) -> f vs
        | OutsideOpenedStore (_, vs) -> f vs

    // Initialization
    let init<'a, 'b when 'a :> IComparable<'a> and 'a :> IEquatable<'a>> stateManager : FabricStore<'a, 'b>= EmptyStore stateManager |> ClosedStore

    let getDictionary (dictionaryName:string) store =
        store
        |> mapValueMethod (fun vs ->
            async {
                match vs with
                | EmptyStore sm -> 
                    let! dict = sm.GetOrAddAsync dictionaryName |> Async.AwaitTask
                    return DictionaryStore (sm,dict)
                | ResultStore (sm,_) -> 
                    let! dict = sm.GetOrAddAsync dictionaryName |> Async.AwaitTask
                    return DictionaryStore (sm,dict)
                | ds -> return ds
            } |> Async.RunSynchronously
        )

    let initDict<'a, 'b when 'a :> IComparable<'a> and 'a :> IEquatable<'a>> stateManager dictionaryName : FabricStore<'a, 'b> = 
        stateManager
        |> init
        |> getDictionary dictionaryName

    // Retrieving the state manager
    let stateManager store =
        store
        |> doValueMethod (fun vs ->
            match vs with
            | EmptyStore sm -> sm
            | DictionaryStore (sm,_) -> sm
            | ResultStore (sm,_) -> sm
        )

    // Transaction scoping
    let useTx tx store = 
        match store with
        | ClosedStore vs -> OutsideOpenedStore (tx, vs)
        | OpenedStore (openTx, vs) ->
            openTx.Dispose()
            OutsideOpenedStore(tx, vs)
        | OutsideOpenedStore (_, vs) -> OutsideOpenedStore (tx, vs)

    let withTx' f (tx:ITransaction) valueStore =
        match valueStore with
        | EmptyStore _ -> f tx Empty
        | DictionaryStore (_, d) -> f tx (Dictionary d)
        | ResultStore (_, v) -> f tx (Values v)

    let withTxClosing f store =
        let sm = stateManager store
        match store with
        | ClosedStore vs ->
            use tx = sm.CreateTransaction()
            withTx' f tx vs
        | OpenedStore (tx, vs) ->
            let result = withTx' f tx vs
            tx.Dispose()
            result
        | OutsideOpenedStore (tx, vs) ->
            // Don't close an external transaction
            withTx' f tx vs

    let withTx f store =
        let sm = stateManager store
        match store with
        | ClosedStore vs ->
            let tx = sm.CreateTransaction()
            let result = withTx' f tx vs
            let resultStore = ResultStore(sm, result)
            OpenedStore (tx, resultStore)
        | OpenedStore (tx, vs) ->
            let result = withTx' f tx vs
            let resultStore = ResultStore(sm, result)
            OpenedStore (tx, resultStore)
        | OutsideOpenedStore (tx, vs) ->
            let result = withTx' f tx vs
            let resultStore = ResultStore(sm, result)
            OutsideOpenedStore (tx, resultStore)

    let withTxAsync' f (tx:ITransaction) valueStore =
        async {
            return!
                match valueStore with
                | EmptyStore _ -> f tx Empty
                | DictionaryStore (_, d) -> f tx (Dictionary d)
                | ResultStore (_, v) -> f tx (Values v)
        }

    let withTxClosingAsync f store =
        async {
            let sm = stateManager store
            match store with
            | ClosedStore vs ->
                use tx = sm.CreateTransaction()
                return! withTxAsync' f tx vs
            | OpenedStore (tx, vs) ->
                let! result = withTxAsync' f tx vs
                tx.Dispose()
                return result
            | OutsideOpenedStore (tx, vs) ->
                // Don't close an external transaction
                return! withTxAsync' f tx vs
        }

    let withTxAsync f store =
        async {
            let sm = stateManager store
            match store with
            | ClosedStore vs ->
                let tx = sm.CreateTransaction()
                let! result = withTxAsync' f tx vs
                let resultStore = ResultStore(sm, result)
                return OpenedStore (tx, resultStore)
            | OpenedStore (tx, vs) ->
                let! result = withTxAsync' f tx vs
                let resultStore = ResultStore(sm, result)
                return OpenedStore (tx, resultStore)
            | OutsideOpenedStore (tx, vs) ->
                let! result = withTxAsync' f tx vs
                let resultStore = ResultStore(sm, result)
                return OpenedStore (tx, resultStore)
        }

    // Enumeration
    let enumerate tx (d:IReliableDictionary<'TKey,'T>) = 
//        let result = 
//            seq {
//                let ct = new CancellationToken()
//                let enumerable = d.CreateEnumerableAsync(tx) |> Async.AwaitTask |> Async.RunSynchronously
//                let enumerator = enumerable.GetAsyncEnumerator()
//                let mutable hasNext = true
//                while (hasNext) do
//                    let next = enumerator.MoveNextAsync(ct) |> Async.AwaitTask |> Async.RunSynchronously
//                    hasNext <- next
//                    yield enumerator.Current.Key,enumerator.Current.Value
//            }
//            |> Seq.toList
//        result |> AsyncSeq.ofSeq
        seq {
            let ct = new CancellationToken()
            let enumerable = d.CreateEnumerableAsync(tx) |> Async.AwaitTask |> Async.RunSynchronously
            let enumerator = enumerable.GetAsyncEnumerator()
            let mutable hasNext = enumerator.MoveNextAsync(ct) |> Async.AwaitTask |> Async.RunSynchronously
            while (hasNext) do
                yield enumerator.Current.Key,enumerator.Current.Value
                let next = enumerator.MoveNextAsync(ct) |> Async.AwaitTask |> Async.RunSynchronously
                hasNext <- next
        } |> Seq.toList |> AsyncSeq.ofSeq

    let all store =
        store
        |> withTx (fun tx values -> 
            match values with
            | Values v -> v
            | Dictionary d -> enumerate tx d
            | Empty -> AsyncSeq.empty
        )

    // Result
    let values store =
        store
        |> withTx (fun tx sv ->
            match sv with
            | Empty -> AsyncSeq.empty
            | Dictionary d -> 
                enumerate tx d
            | Values v -> v
        )
        |> doValueMethod (fun vs ->
            match vs with
            | EmptyStore _ -> AsyncSeq.empty
            | DictionaryStore _ -> AsyncSeq.empty
            | ResultStore (_,v) -> v
        )

    // Applicative functor ('map')
    let result sm v = ResultStore (sm, v) |> ClosedStore

    let result' sm v = 
        match v with
        | None -> EmptyStore sm |> ClosedStore
        | Some [] -> EmptyStore sm |> ClosedStore
        | Some sequence -> result sm (sequence |> AsyncSeq.ofSeq)

    let map f store =
        store
        |> withTx (fun tx sv ->
            match sv with
            | Empty -> AsyncSeq.empty
            | Dictionary d ->
                AsyncSeq.map f <| enumerate tx d
            | Values v ->
                AsyncSeq.map f v
        )

    // Map all AsyncSeq functions into FabricStore land
    let private mapAsyncSeq f =
        withTx (fun tx storeValues ->
            match storeValues with
            | Values sequence ->
//                let first = sequence |> AsyncSeq.map fst
//                let second = sequence |> AsyncSeq.map snd
//                let secondMapped = f second
//                AsyncSeq.zip first secondMapped
                sequence |> f
            | Empty -> AsyncSeq.empty
            | Dictionary d -> 
                let sequence = enumerate tx d
//                let first = sequence |> AsyncSeq.map fst
//                let second = sequence |> AsyncSeq.map snd
//                let secondMapped = f second
//                AsyncSeq.zip first secondMapped
                sequence |> f
        )

    let private doAsyncSeq defaultValue f =
        withTxClosing (fun tx storeValues ->
            match storeValues with
            | Empty -> defaultValue
            | Dictionary d ->
                let sequence = enumerate tx d
                let second = sequence |> AsyncSeq.map snd
                f second
            | Values v -> f (AsyncSeq.map snd v)
        )

    let private doAsyncSeq' defaultValue f =
        withTxClosing (fun tx storeValues ->
            match storeValues with
            | Empty -> defaultValue
            | Dictionary d ->
                let sequence = enumerate tx d
                f sequence
            | Values v -> f v
        )

    let private lift2AsyncSeq f store1 store2 =
        doAsyncSeq' (f AsyncSeq.empty) f store1
        |> fun f' -> doAsyncSeq' (f' AsyncSeq.empty) f' store2
        |> result (store1 |> stateManager)
        
    let private doAsyncSeqAsync defaultValue f = 
        withTxClosingAsync (fun tx storeValues ->
            async {
                match storeValues with
                | Empty -> return defaultValue
                | Dictionary d ->
                    let sequence = enumerate tx d
                    let second = sequence |> AsyncSeq.map snd
                    return! f second
                | Values v -> return! f (AsyncSeq.map snd v)
            }
        )

    // Methods lifted from AsyncSeq
    let append item store = store |> (mapAsyncSeq <| AsyncSeq.append item)
    let cache store = store |> (mapAsyncSeq <| AsyncSeq.cache)
    let choose chooser store = store |> (mapAsyncSeq <| AsyncSeq.choose chooser)
    let chooseAsync chooser store = store |> (mapAsyncSeq <| AsyncSeq.chooseAsync chooser)
    let contains value store = store |> (doAsyncSeqAsync false <| (AsyncSeq.contains value))
    let distinctUntilChanged store = store |> (mapAsyncSeq <| AsyncSeq.distinctUntilChanged)
    let distinctUntilChangedWith mapping store = store |> (mapAsyncSeq <| AsyncSeq.distinctUntilChangedWith mapping)
    let distinctUntilChangedWithAsync mapping store = store |> (mapAsyncSeq <| AsyncSeq.distinctUntilChangedWithAsync mapping)
    let exists predicate store = store |> (doAsyncSeqAsync false <| AsyncSeq.exists predicate)
    let filter predicate store = store |> (mapAsyncSeq <| AsyncSeq.filter predicate)
    let filterAsSeq predicate store = store |> (mapAsyncSeq <| AsyncSeq.filter predicate) |> values |> AsyncSeq.toBlockingSeq
    let filterAsync predicate store = store |> (mapAsyncSeq <| AsyncSeq.filterAsync predicate)
    let firstOrDefault ``default`` = doAsyncSeqAsync ``default`` <| AsyncSeq.firstOrDefault ``default``
    let fold folder state store = store |> (doAsyncSeqAsync state <| AsyncSeq.fold folder state)
    let foldAsync folder state store = store |> (doAsyncSeqAsync state <| AsyncSeq.foldAsync folder state)
    let forall predicate store = store |> (doAsyncSeqAsync false <| AsyncSeq.forall predicate)
//    let indexed store = store |> (mapAsyncSeq <| AsyncSeq.indexed)
//    let interleave = lift2AsyncSeq <| AsyncSeq.interleave
//    let interleaveChoice = lift2AsyncSeq <| AsyncSeq.interleaveChoice
    let iter action store = store |> (doAsyncSeqAsync () <| AsyncSeq.iter action)
    let iterAsync action store = store |> (doAsyncSeqAsync () <| AsyncSeq.iterAsync action)
    let lastOrDefault ``default`` store = store |> (doAsyncSeqAsync ``default`` <| AsyncSeq.lastOrDefault ``default``)
    let length store = 
        store
        |> withTxClosing (fun tx values ->
            match values with
            | Empty -> async { return 0L }
            | Dictionary d -> d.GetCountAsync(tx) |> Async.AwaitTask
            | Values v -> v |> AsyncSeq.length
        )
    let mapAsSeq mapping store = store |> map mapping |> values |> AsyncSeq.toBlockingSeq
    let mapAsync mapping store = store |> (mapAsyncSeq <| AsyncSeq.mapAsync mapping)
    let mapiAsync mapping store = store |> (mapAsyncSeq <| AsyncSeq.mapiAsync mapping)
    let merge store = store |> (lift2AsyncSeq <| AsyncSeq.merge)
    // Do this one
//    let mergeAll list store = AsyncSeq.fold (EmptyStore (store |>)
//    let mergeChoice store = store |> (lift2AsyncSeq <| AsyncSeq.mergeChoice)
//    let pairwise store = store |> (mapAsyncSeq <| AsyncSeq.pairwise)
    let replicate count value sm = AsyncSeq.replicate count value |> result sm
    let replicateInfinite value sm = AsyncSeq.replicateInfinite value |> result sm
    let scan folder state store = store |> (mapAsyncSeq <| AsyncSeq.scan folder state)
    let scanAsync folder state store = store |> (mapAsyncSeq <| AsyncSeq.scanAsync folder state)
    let singleton value sm = AsyncSeq.singleton value |> result sm
    let skip count store = store |> (mapAsyncSeq <| AsyncSeq.skip count)
    let skipUntilSignal signal store = store |> (mapAsyncSeq <| AsyncSeq.skipUntilSignal signal)
    let skipWhile predicate = mapAsyncSeq <| AsyncSeq.skipWhile predicate
    let skipWhileAsync predicate = mapAsyncSeq <| AsyncSeq.skipWhileAsync predicate
//    let sum = (doAsyncSeqAsync <| AsyncSeq.sum)
    let take count = mapAsyncSeq <| AsyncSeq.take count
    let truncate count = 
        mapAsyncSeq <| (AsyncSeq.toBlockingSeq
                       >> Seq.truncate count
                       >> AsyncSeq.ofSeq)
                                        
    let takeUntilSignal signal = mapAsyncSeq <| AsyncSeq.takeUntilSignal signal
    let takeWhile predicate = mapAsyncSeq <| AsyncSeq.takeWhile predicate
    let takeWhileAsync predicate = mapAsyncSeq <| AsyncSeq.takeWhileAsync predicate
    let threadStateAsync folder state = mapAsyncSeq <| AsyncSeq.threadStateAsync folder state
    let toArray store = store |> (doAsyncSeq Array.empty <| AsyncSeq.toArray)
    let toArrayAsync store = store |> (doAsyncSeqAsync Array.empty <| AsyncSeq.toArrayAsync)
    let toBlockingSeq store = store |> (doAsyncSeq Seq.empty <| AsyncSeq.toBlockingSeq)
    let toList store = store |> (doAsyncSeq List.empty <| AsyncSeq.toList)
    let toListAsync store = store |> (doAsyncSeqAsync List.empty <| AsyncSeq.toListAsync)
    let toSeq store = store |> (doAsyncSeq <| AsyncSeq.toBlockingSeq)
    let tryFind predicate = doAsyncSeqAsync None <| AsyncSeq.tryFind predicate
    let tryFirst store = store |> (doAsyncSeqAsync None <| AsyncSeq.tryFirst)
    let tryLast store =  store |> (doAsyncSeqAsync None <| AsyncSeq.tryLast)
    let tryPick chooser = doAsyncSeqAsync None <| AsyncSeq.tryPick chooser
//    let unfold predicate = mapAsyncSeq <| AsyncSeq.unfold predicate
//    let unfoldAsync predicate = mapAsyncSeq <| AsyncSeq.unfoldAsync predicate
    let zapp functions = mapAsyncSeq <| AsyncSeq.zapp functions
    let zappAsync functions = mapAsyncSeq <| AsyncSeq.zappAsync functions
//    let zip store = store |> (lift2AsyncSeq <| AsyncSeq.zip)
    let zipWith mapping = lift2AsyncSeq <| AsyncSeq.zipWith mapping
    let zipWithAsync mapping = lift2AsyncSeq <| AsyncSeq.zipWithAsync mapping

    // "Ultra-pure" methods: ones which do not enumerate the store at all
    let tryGetValueAsync key store = 
        store
        |> withTxAsync (fun tx sv ->
            async {
                match sv with
                | Empty -> return AsyncSeq.empty
                | Dictionary d ->
                    let! item = d.TryGetValueAsync(tx, key) |> Async.AwaitTask
                    if item.HasValue
                        then return AsyncSeq.singleton (key, item.Value)
                        else return AsyncSeq.empty
                | Values v ->
                    let! item = AsyncSeq.tryFind (fst>>(=)key) v
                    match item with
                    | Some i -> return AsyncSeq.singleton i
                    | None -> return AsyncSeq.empty
            }
        )

    let updateValueAsync key value store =
        store
        |> withTxAsync (fun tx sv ->
            async {
                match sv with
                | Empty -> return AsyncSeq.empty
                | Dictionary d ->
                    do! d.AddOrUpdateAsync(tx, key, value, Func<_,_,_>(fun _ _ -> value)) |> Async.AwaitTask
                    return AsyncSeq.empty
                | Values _ ->
                    return AsyncSeq.empty
            }
        )

    let addValueAsync key value store =
        store
        |> withTxAsync (fun tx sv ->
            async {
                match sv with
                | Empty -> return AsyncSeq.empty
                | Dictionary d ->
                    do! d.AddAsync(tx, key, value) |> Async.AwaitTask
                    return AsyncSeq.empty
                | Values _ ->
                    return AsyncSeq.empty
            }
        )

    // Staging 
    let stageOne key item store =
        store
        |> mapValueMethod (fun vs ->
            match vs with
            | EmptyStore sm ->
                ResultStore(sm, AsyncSeq.singleton (key,item))
            | DictionaryStore (sm,_) ->
                ResultStore(sm, AsyncSeq.singleton (key,item))
            | ResultStore (sm,v) ->
                let sequence = (AsyncSeq.singleton (key,item)) |> AsyncSeq.append v
                ResultStore(sm, sequence)
        )
    let stageMany values store = 
        store
        |> mapValueMethod (fun vs ->
            match vs with
            | EmptyStore sm ->
                ResultStore(sm, values)
            | DictionaryStore (sm,_) ->
                ResultStore(sm, values)
            | ResultStore (sm,v) ->
                let sequence = AsyncSeq.append v values
                ResultStore(sm, sequence)
        )

    // Crud operations
    let create (dictionaryName:string) store =
        let sm = stateManager store
        store
        |> withTxAsync (fun tx storeValues ->
            async {
                match storeValues with
                | Empty -> return AsyncSeq.empty
                | Dictionary _ -> return AsyncSeq.empty // Don't create a dictionary version, that would just duplicate items
                | Values values ->
                    let! (dictionary:IReliableDictionary<_,_>) = sm.GetOrAddAsync(dictionaryName) |> Async.AwaitTask
                    do!
                        values
                        |> AsyncSeq.iterAsync (fun v ->
                            async{
                                do! dictionary.AddAsync(tx, fst v, snd v) |> Async.AwaitTask
                            }
                        )
                    return values
            }
        )

    let update (dictionaryName:string) store =
        let sm = stateManager store
        store
        |> withTxAsync (fun tx storeValues ->
            async {
                match storeValues with
                | Empty -> return AsyncSeq.empty
                | Dictionary _ -> return AsyncSeq.empty // Don't create a dictionary version, that would just duplicate items
                | Values values ->
                    let! (dictionary:IReliableDictionary<_,_>) = sm.GetOrAddAsync(dictionaryName) |> Async.AwaitTask
                    do!
                        values
                        |> AsyncSeq.iterAsync (fun (key, value) ->
                            async{
                                let! updatedValue = dictionary.AddOrUpdateAsync(tx, key, value, Func<_,_,_>(fun _ _ -> value)) |> Async.AwaitTask
                                updatedValue |> ignore
                            }
                        )
                    return values
            }
        )

    let delete (dictionaryName:string) store =
        let sm = stateManager store
        store
        |> withTxAsync (fun tx storeValues ->
            async {
                match storeValues with
                | Empty -> return AsyncSeq.empty
                | Dictionary _ -> return AsyncSeq.empty // Don't create a dictionary version, that would just duplicate items
                | Values values ->
                    let! (dictionary:IReliableDictionary<_,_>) = sm.GetOrAddAsync(dictionaryName) |> Async.AwaitTask
                    do!
                        values
                        |> AsyncSeq.iterAsync (fun v ->
                            async{
                                let! result = dictionary.TryRemoveAsync(tx, fst v) |> Async.AwaitTask
                                result |> ignore
                            }
                        )
                    return AsyncSeq.empty
            }
        )

    let commit store =
        async {
            match store with
            | OutsideOpenedStore (tx,_) -> 
                do! tx.CommitAsync() |> Async.AwaitTask
                return store
            | OpenedStore (tx,vs) -> 
                do! tx.CommitAsync() |> Async.AwaitTask
                tx.Dispose()
                return ClosedStore(vs)
            | ClosedStore _ -> return store
        }

    let close store =
        async {
            match store with
            | OutsideOpenedStore (_, vs) ->
                // Don't close a transaction opened outside of this store
                return ClosedStore(vs)
            | OpenedStore (tx, vs) ->
                tx.Dispose()
                return ClosedStore(vs)
            | ClosedStore _ -> return store
        }
