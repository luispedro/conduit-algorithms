{- Copyright 2017 Luis Pedro Coelho
 - License: MIT
 -}
{-# LANGUAGE TemplateHaskell, QuasiQuotes, FlexibleContexts, OverloadedStrings #-}
module Main where

import Test.Framework.TH
import Test.HUnit
import Test.Framework.Providers.HUnit

import qualified Data.ByteString as B
import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as CC
import           Data.Conduit ((.|))

import qualified Data.Conduit.Algorithms as CAlg
import qualified Data.Conduit.Algorithms.Utils as CAlg
import qualified Data.Conduit.Algorithms.Async as CAlg
import           System.Directory (removeFile)

main :: IO ()
main = $(defaultMainGenerator)

testingFileNameGZ :: FilePath
testingFileNameGZ = "file_just_for_testing_delete_me_please.gz"

extract c = C.runConduitPure (c .| CC.sinkList)

extractIO c = C.runConduitRes (c .| CC.sinkList)

case_uniqueC = extract (CC.yieldMany [1,2,3,1,1,2,3] .| CAlg.uniqueC) @=? [1,2,3 :: Int]
case_mergeC = extract (CAlg.mergeC [CC.yieldMany [0, 2, 4], CC.yieldMany [1,3,4,5]]) @=? [0,1,2,3,4,4,5 :: Int]
case_groupC = extract (CC.yieldMany [0..10] .| CAlg.groupC 3) @=? [[0,1,2], [3,4,5], [6,7,8], [9, 10 :: Int]]

case_asyncMap :: IO ()
case_asyncMap = do
    vals <- extractIO (CC.yieldMany [0..10] .| CAlg.asyncMapC 3 (+ (1:: Int)))
    (vals @=? [1..11])

case_asyncGzip :: IO ()
case_asyncGzip = do
    C.runConduitRes (CC.yieldMany ["Hello", " ", "World"] .| CAlg.asyncGzipToFile testingFileNameGZ)
    r <- B.concat <$> (extractIO (CAlg.asyncGzipFromFile testingFileNameGZ))
    r @=? "Hello World"
    removeFile testingFileNameGZ

