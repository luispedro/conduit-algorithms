{- Copyright 2018
 - Licence: MIT -}
import Criterion.Main (defaultMain, nfIO, bench, bgroup)
import Control.Monad.Trans.Resource


import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))



import qualified Data.Conduit.Algorithms as CAlg
import qualified Data.Conduit.Algorithms.Storable as CAlg
import qualified Data.Conduit.Algorithms.Utils as CAlg
import qualified Data.Conduit.Algorithms.Async as CAlg
import qualified Data.Conduit.Algorithms.Async.ByteString as CAlg



progress :: Monad m => Int -> Int -> C.ConduitT () Int m ()
progress _ 0 = return ()
progress n i = C.yield n >> progress (n + 3) (i - 1)

nfConduit = nfIO . C.runConduitRes

main = defaultMain [
    bgroup "merge"
        [ bench "mergeC2" $ nfConduit (CAlg.mergeC2 (progress 1 1000) (progress 30 2000) .| CL.fold (+) (0 :: Int))
        , bench "mergeC_3" $ nfConduit (CAlg.mergeC [progress 1 1000, progress 30 2000, progress 7 9] .| CL.fold (+) (0 :: Int))
        ]
    ]
