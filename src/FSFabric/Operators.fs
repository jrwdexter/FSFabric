namespace FSFabric

module Async =
    let map f v =
        async {
            let! value = v
            return f value
        }

    let bind f xAsync =
        async {
            let! x = xAsync
            return! f x
        }

    module Operators =
        let inline (>>=) m f = bind f m