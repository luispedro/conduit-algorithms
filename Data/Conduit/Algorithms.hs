{-|
Module      : Data.Conduit.Algorithms
Copyright   : 2013-2017 Luis Pedro Coelho
License     : MIT
Maintainer  : luis@luispedro.org

Simple algorithms packaged as Conduits
-}


{-# LANGUAGE Rank2Types #-}
module Data.Conduit.Algorithms
    ( uniqueOnC
    , uniqueC
    , removeRepeatsC
    , mergeC
    , mergeC2
    ) where

import qualified Data.Conduit as C
import qualified Data.Conduit.Internal as CI
import qualified Data.Set as S
import           Control.Monad.Trans.Class (lift)

import           Data.Conduit.Algorithms.Utils (awaitJust)


-- | Unique conduit.
--
-- For each element, it checks its key (using the @a -> b@ key function) and
-- yields it if it has not seen it before.
--
-- Note that this conduit /does not/ assume that the input is sorted. Instead
-- it uses a 'Data.Set' to store previously seen elements. Thus, memory usage
-- is O(N) and time is O(N log N). If the input is sorted, you can use
-- 'removeRepeatsC'
uniqueOnC :: (Ord b, Monad m) => (a -> b) -> C.Conduit a m a
uniqueOnC f = checkU (S.empty :: S.Set b)
    where
        checkU cur = awaitJust $ \val ->
                        if f val `S.member` cur
                            then checkU cur
                            else do
                                C.yield val
                                checkU (S.insert (f val) cur)
-- | Unique conduit
--
-- See 'uniqueOnC' and 'removeRepeatsC'
uniqueC :: (Ord a, Monad m) => C.Conduit a m a
uniqueC = uniqueOnC id

-- | Removes repeated elements
--
-- @
--  yieldMany [0, 0, 1, 1, 1, 2, 2, 0] .| removeRepeatsC .| consume
-- @
--
-- is equivalent to @[0, 1, 2, 0]@
--
-- See 'uniqueC' and 'uniqueOnC'
removeRepeatsC :: (Eq a, Monad m) => C.Conduit a m a
removeRepeatsC = awaitJust removeRepeatsC'
    where
        removeRepeatsC' prev = C.await >>= \case
                                        Nothing -> C.yield prev
                                        Just next
                                            | next == prev -> removeRepeatsC' prev
                                            | otherwise -> do
                                                        C.yield prev
                                                        removeRepeatsC' next


-- | Merge a list of sorted sources to produce a single (sorted) source
--
-- This takes a list of sorted sources and produces a 'C.Source' which outputs
-- all elements in sorted order.
--
-- See 'mergeC2'
mergeC :: (Ord a, Monad m) => [C.Source m a] -> C.Source m a
mergeC [] = return ()
mergeC [s] = s
mergeC [a,b] = mergeC2 a b
mergeC args = mergeC2 (mergeC right) (mergeC left)
    where
        right = take n args
        left = drop n args
        n = (length args) `div` 2

-- | Take two sorted sources and merge them.
--
-- See 'mergeC'
mergeC2 :: (Ord a, Monad m) => C.Source m a -> C.Source m a -> C.Source m a
mergeC2 (CI.ConduitM s1) (CI.ConduitM s2) = CI.ConduitM $ \rest -> let
        go right@(CI.HaveOutput s1' f1 v1) left@(CI.HaveOutput s2' f2 v2)
            | compare v1 v2 /= GT = CI.HaveOutput (go s1' left) (f1 >> f2) v1
            | otherwise = CI.HaveOutput (go right s2') (f1 >> f2) v2
        go right CI.Done{} = right
        go CI.Done{} left = left
        go (CI.PipeM p) left = do
            next <- lift p
            go next left
        go right (CI.PipeM p) = do
            next <- lift p
            go right next
        go (CI.NeedInput _ next) left = go (next ()) left
        go right (CI.NeedInput _ next) = go right (next ())
        go (CI.Leftover next ()) left = go next left
        go right (CI.Leftover next ()) = go right next
    in go (s1 rest) (s2 rest)

