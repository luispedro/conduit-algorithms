{-|
Module      : Data.Conduit.Algorithms.Utils
Copyright   : 2013-2019 Luis Pedro Coelho
License     : MIT
Maintainer  : luis@luispedro.org

A few miscellaneous conduit utils
-}
module Data.Conduit.Algorithms.Utils
    ( awaitJust
    , enumerateC
    , groupC
    ) where

import qualified Data.Conduit as C
import           Data.Maybe (maybe)
import           Control.Monad (unless)

-- | Act on the next input (do nothing if no input). @awaitJust f@ is equivalent to
--
--
-- @ do
--      next <- C.await
--      case next of
--          Just val -> f val
--          Nothing -> return ()
-- @
--
-- This is a simple utility adapted from
-- http://neilmitchell.blogspot.de/2015/07/thoughts-on-conduits.html
awaitJust :: Monad m => (a -> C.ConduitT a b m ()) -> C.ConduitT a b m ()
awaitJust f = C.await >>= maybe (return ()) f
{-# INLINE awaitJust #-}

-- | Conduit analogue to Python's enumerate function
enumerateC :: Monad m => C.ConduitT a (Int, a) m ()
enumerateC = enumerateC' 0
    where
        enumerateC' !i = awaitJust $ \v -> do
                                        C.yield (i, v)
                                        enumerateC' (i + 1)
{-# INLINE enumerateC #-}

-- | groupC yields the input as groups of 'n' elements. If the input is not a
-- multiple of 'n', the last element will be incomplete
--
-- Example:
--
-- @
--      CC.yieldMany [0..10] .| groupC 3 .| CC.consumeList
-- @
--
-- results in @[ [0,1,2], [3,4,5], [6,7,8], [9, 10] ]@
--
-- This function is deprecated; use 'Data.Conduit.List.chunksOf'
groupC :: (Monad m) => Int -> C.ConduitT a [a] m ()
groupC n = loop n []
    where
        loop 0 ps = C.yield (reverse ps) >> loop n []
        loop c ps = C.await >>= \case
            Nothing -> unless (null ps) $ C.yield (reverse ps)
            Just p -> loop (c-1) (p:ps)
{-# WARNING groupC "This function is deprecated; use 'Data.Conduit.List.chunksOf'" #-}
