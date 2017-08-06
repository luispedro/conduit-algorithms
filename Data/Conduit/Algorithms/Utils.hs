{- Copyright 2013-2017 Luis Pedro Coelho
 - License: MIT -}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts, CPP #-}

module Data.Conduit.Algorithms.Utils
    ( awaitJust
    , groupC
    ) where

import qualified Data.Conduit as C
import           Data.Maybe (maybe)
import           Control.Monad (unless)

-- | This is a simple utility adapted from
-- http://neilmitchell.blogspot.de/2015/07/thoughts-on-conduits.html
awaitJust :: Monad m => (a -> C.Conduit a m b) -> C.Conduit a m b
awaitJust f = C.await >>= maybe (return ()) f

-- | groupC yields the input as groups of 'n' elements. If the input is not a
-- multiple of 'n', the last element will be incomplete
groupC :: (Monad m) => Int -> C.Conduit a m [a]
groupC n = loop n []
    where
        loop 0 ps = C.yield (reverse ps) >> loop n []
        loop c ps = C.await >>= \case
            Nothing -> unless (null ps) $ C.yield (reverse ps)
            Just p -> loop (c-1) (p:ps)

