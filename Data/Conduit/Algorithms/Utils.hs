{-|
Module      : Data.Conduit.Algorithms.Utils
Copyright   : 2013-2017 Luis Pedro Coelho
License     : MIT
Maintainer  : luis@luispedro.org

A few miscellaneous conduit utils
-}
module Data.Conduit.Algorithms.Utils
    ( awaitJust
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
awaitJust :: Monad m => (a -> C.Conduit a m b) -> C.Conduit a m b
awaitJust f = C.await >>= maybe (return ()) f

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
groupC :: (Monad m) => Int -> C.Conduit a m [a]
groupC n = loop n []
    where
        loop 0 ps = C.yield (reverse ps) >> loop n []
        loop c ps = C.await >>= \case
            Nothing -> unless (null ps) $ C.yield (reverse ps)
            Just p -> loop (c-1) (p:ps)

