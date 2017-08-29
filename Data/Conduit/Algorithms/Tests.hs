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
import           Data.List (sort)
import           System.Directory (removeFile)

import qualified Data.Conduit.Algorithms as CAlg
import qualified Data.Conduit.Algorithms.Utils as CAlg
import qualified Data.Conduit.Algorithms.Async as CAlg

main :: IO ()
main = $(defaultMainGenerator)

testingFileNameGZ :: FilePath
testingFileNameGZ = "file_just_for_testing_delete_me_please.gz"

extract c = C.runConduitPure (c .| CC.sinkList)

extractIO c = C.runConduitRes (c .| CC.sinkList)

shouldProduce values cond = extract cond @?= values

case_uniqueC = extract (CC.yieldMany [1,2,3,1,1,2,3] .| CAlg.uniqueC) @=? [1,2,3 :: Int]
case_mergeC = shouldProduce expected $
                            CAlg.mergeC
                                [ CC.yieldMany i1
                                , CC.yieldMany i2
                                , CC.yieldMany i3
                                ]
    where
        expected = sort (concat [i1, i2, i3])
        i1 = [ 0, 2, 4 :: Int]
        i2 = [ 1, 3, 4, 5]
        i3 = [-1, 0, 7]

case_mergeC2 = shouldProduce [0, 1, 1, 2, 3, 5 :: Int] $
                            CAlg.mergeC2
                                (CC.yieldMany [0, 1, 2])
                                (CC.yieldMany [1, 3, 5])

case_mergeC2same = shouldProduce [0, 0, 1, 1, 2, 2 :: Int] $
                            CAlg.mergeC2
                                (CC.yieldMany [0, 1, 2])
                                (CC.yieldMany [0, 1, 2])

case_groupC = shouldProduce [[0,1,2], [3,4,5], [6,7,8], [9, 10 :: Int]] $
                            CC.yieldMany [0..10] .| CAlg.groupC 3

case_removeRepeatsC = shouldProduce [0,1,2,3,4,5,6,7,8,9, 10 :: Int] $
                            CC.yieldMany [0,0,0,1,1,1,2,2,3,4,5,6,6,6,6,7,7,8,9,10,10] .| CAlg.removeRepeatsC

case_asyncMap :: IO ()
case_asyncMap = do
    vals <- extractIO (CC.yieldMany [0..10] .| CAlg.asyncMapC 3 (+ (1:: Int)))
    (vals @?= [1..11])

case_asyncGzip :: IO ()
case_asyncGzip = do
    C.runConduitRes (CC.yieldMany ["Hello", " ", "World"] .| CAlg.asyncGzipToFile testingFileNameGZ)
    r <- B.concat <$> (extractIO (CAlg.asyncGzipFromFile testingFileNameGZ))
    r @?= "Hello World"
    removeFile testingFileNameGZ

