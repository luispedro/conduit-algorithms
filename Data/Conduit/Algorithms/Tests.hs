{- Copyright 2013-2017 NGLess Authors
 - License: MIT
 -}
{-# LANGUAGE TemplateHaskell, QuasiQuotes #-}
module Main where

import Test.Framework.TH
import Test.HUnit
import Test.Framework.Providers.HUnit

import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as CC
import           Data.Conduit ((.|))

import qualified Data.Conduit.Algorithms as CAlg

main :: IO ()
main = $(defaultMainGenerator)

case_unique = C.runConduitPure (CC.yieldMany [1,2,3,1,1,2,3] .| CAlg.uniqueC .| CC.sinkList) @=? [1,2,3 :: Int]
