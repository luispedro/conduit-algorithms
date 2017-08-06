{-# LANGUAGE Rank2Types #-}
module Data.Conduit.Algorithms
    ( uniqueOnC
    , uniqueC
    , mergeC
    ) where

import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as CC
import qualified Data.Set as S
import           Control.Monad.Trans.Class (lift)

import           Data.Conduit.Algorithms.Utils (awaitJust)

uniqueOnC :: (Ord b, Monad m) => (a -> b) -> C.Conduit a m a
uniqueOnC f = checkU (S.empty :: S.Set b)
    where
        checkU cur = awaitJust $ \val ->
                        if f val `S.member` cur
                            then checkU cur
                            else do
                                C.yield val
                                checkU (S.insert (f val) cur)
uniqueC :: (Ord a, Monad m) => C.Conduit a m a
uniqueC = uniqueOnC id

mergeC :: (Ord a, Monad m) => [C.Source m a] -> C.Source m a
mergeC [] = return ()
mergeC [s] = s
mergeC [a,b] = mergeC2 a b
mergeC args = let (a,b) = split2 args in mergeC2 (mergeC a) (mergeC b)
    where
        split2 :: [a] -> ([a],[a])
        split2 [] = ([], [])
        split2 [a] = ([a], [])
        split2 [a,b] = ([a], [b])
        split2 (x:y:rs) = let (xs,ys) = split2 rs in (x:xs, y:ys)

mergeC2 :: (Ord a, Monad m) => C.Source m a -> C.Source m a -> C.Source m a
mergeC2 s1 s2 = do
        (c1', e1) <- lift $ s1 C.$$+ CC.head
        (c2', e2) <- lift $ s2 C.$$+ CC.head
        continue c1' c2' e1 e2
    where
        continue :: (Monad m, Ord a) => C.ResumableSource m a -> C.ResumableSource m a -> Maybe a -> Maybe a -> C.Source m a
        continue _ _ Nothing Nothing = return ()
        continue _ c Nothing e = continue c undefined e Nothing
        continue c _ (Just e) Nothing = do
            C.yield e
            yieldAll c
        continue c1 c2 je1@(Just e1) je2@(Just e2)
            | compare e1 e2 == GT = continue c2 c1 je2 je1
            | otherwise = do
                C.yield e1
                (c1', e1') <- lift $ c1 C.$$++ CC.head
                continue c1' c2 e1' (Just e2)
        yieldAll :: (Monad m) => C.ResumableSource m a -> C.Source m a
        yieldAll rs = do
            (rs',v) <- lift $ rs C.$$++ CC.head
            case v of
                Just v' -> do
                    C.yield v'
                    yieldAll rs'
                Nothing -> return ()
