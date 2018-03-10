{-|
Module      : Data.Conduit.Algorithms.Async
Copyright   : 2013-2017 Luis Pedro Coelho
License     : MIT
Maintainer  : luis@luispedro.org

Higher level async processing interfaces.
-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts, CPP, TupleSections #-}

module Data.Conduit.Algorithms.Async
    ( conduitPossiblyCompressedFile
    , conduitPossiblyCompressedToFile
    , asyncMapC
    , asyncMapEitherC
    , asyncGzipTo
    , asyncGzipToFile
    , asyncGzipFrom
    , asyncGzipFromFile
    , asyncBzip2To
    , asyncBzip2ToFile
    , asyncBzip2From
    , asyncBzip2FromFile
    , asyncXzTo
    , asyncXzToFile
    , asyncXzFrom
    , asyncXzFromFile
    , unorderedAsyncMapC
    ) where


import qualified Data.ByteString as B
import qualified Control.Concurrent.Async as A
import qualified Control.Concurrent.STM.TBQueue as TQ
import           Control.Concurrent.STM (atomically)

import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.Async as CA
import qualified Data.Conduit.TQueue as CA
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Zlib as CZ
import qualified Data.Conduit.Lzma as CX
import qualified Data.Streaming.Zlib as SZ
import qualified Data.Conduit.BZlib as CZ
import qualified Data.Conduit as C
import           Data.Conduit ((.|))

import qualified Data.Sequence as Seq
import           Data.Sequence ((|>), ViewL(..))
import           Data.Foldable (toList)
import           Control.Monad (forM_)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Error.Class (MonadError(..))
import           Control.Monad.Trans.Resource (MonadResource, MonadBaseControl)
import           Control.Exception (evaluate, displayException)
import           Control.DeepSeq (NFData, force)
import           System.IO.Error (mkIOError, userErrorType)
import           System.IO
import           Data.List (isSuffixOf)
import           Data.Conduit.Algorithms.Utils (awaitJust)



-- | This is like 'Data.Conduit.List.map', except that each element is processed
-- in a separate thread (up to 'maxThreads' can be queued up at any one time).
-- Results are evaluated to normal form (not weak-head normal form!, i.e., the
-- structure is deeply evaluated) to ensure that the computation is fully
-- evaluated in the worker thread.
--
-- Note that there is some overhead in threading. It is often a good idea to
-- build larger chunks of input before passing it to 'asyncMapC' to amortize
-- the costs. That is, when @f@ is not a lot of work, instead of @asyncMapC f@,
-- it is sometimes better to do
--
-- @
--    CC.conduitVector 4096 .| asyncMapC (V.map f) .| CC.concat
-- @
--
-- where @CC@ refers to 'Data.Conduit.Combinators'
--
-- See 'unorderedAsyncMapC'
asyncMapC :: forall a m b . (MonadIO m, NFData b) =>
                    Int -- ^ Maximum number of worker threads
                    -> (a -> b) -- ^ Function to execute
                    -> C.Conduit a m b
asyncMapC = asyncMapCHelper True

-- | A version of 'asyncMapC' which can reorder results in the stream
--
-- If the order of the results is not important, this function can lead to a
-- better use of resources if some of the chunks take longer to complete.
--
-- See 'asyncMapC'
unorderedAsyncMapC :: forall a m b . (MonadIO m, NFData b) =>
                    Int -- ^ Maximum number of worker threads
                    -> (a -> b) -- ^ Function to execute
                    -> C.Conduit a m b
unorderedAsyncMapC = asyncMapCHelper False

asyncMapCHelper  :: forall a m b . (MonadIO m, NFData b) =>
                    Bool
                    -> Int -- ^ Maximum number of worker threads
                    -> (a -> b) -- ^ Function to execute
                    -> C.Conduit a m b
asyncMapCHelper isSynchronous maxThreads f = initLoop (0 :: Int) (Seq.empty :: Seq.Seq (A.Async b))
    where
        initLoop :: Int -> Seq.Seq (A.Async b) -> C.Conduit a m b
        initLoop size q
            | size == maxThreads = loop q
            | otherwise = C.await >>= \case
                Nothing -> yAll q
                Just v -> do
                        v' <- sched v
                        initLoop (size + 1) (q |> v')
        sched :: a -> C.ConduitM a b m (A.Async b)
        sched v = liftIO . A.async . evaluate . force $ f v

        -- | yield all
        yAll :: Seq.Seq (A.Async b) -> C.Conduit a m b
        yAll q = case Seq.viewl q of
            EmptyL -> return ()
            v :< rest -> (liftIO (A.wait v) >>= yieldOrCleanup rest) >> yAll rest

        loop :: Seq.Seq (A.Async b) -> C.Conduit a m b
        loop q = C.await >>= \case
                Nothing -> yAll q
                Just v -> do
                    v' <- sched v
                    (r, q') <- liftIO $ retrieveResult q
                    yieldOrCleanup q' r
                    loop (q' |> v')
        cleanup :: Seq.Seq (A.Async b) -> m ()
        cleanup q = liftIO $ forM_ q A.cancel
        yieldOrCleanup q = flip C.yieldOr (cleanup q)

        retrieveResult :: Seq.Seq (A.Async b) -> IO (b, Seq.Seq (A.Async b))
        retrieveResult q
            | isSynchronous = case Seq.viewl q of
                        (r :< rest) -> (, rest) <$> A.wait r
                        _ -> error "Impossible situation"
            | otherwise = do
                (k, r) <- liftIO (A.waitAny (toList q))
                return (r, Seq.filter (/= k) q)


-- | 'asyncMapC' with error handling. The inner function can now return an
-- error (as a 'Left'). When the first error is seen, it 'throwError's in the
-- main monad. Note that 'f' may be evaluated for arguments beyond the first
-- error (as some threads may be running in the background and already
-- processing elements after the first error).
--
-- See 'asyncMapC'
asyncMapEitherC :: forall a m b e . (MonadIO m, NFData b, NFData e, MonadError e m) => Int -> (a -> Either e b) -> C.Conduit a m b
asyncMapEitherC maxThreads f = asyncMapC maxThreads f .| (C.awaitForever $ \case
                                Right v -> C.yield v
                                Left err -> throwError err)


-- | concatenates input into larger chunks and yields it. Its indended use is
-- to build up larger blocks from smaller ones so that they can be sent across
-- thread barriers with little overhead.
--
-- the chunkSize parameter is a hint, not an exact element. In particular,
-- larger chunks are not split up and smaller chunks can be yielded too.
bsConcatTo :: MonadIO m => Int -- ^ chunk hint
                            -> C.Conduit B.ByteString m [B.ByteString]
bsConcatTo chunkSize = awaitJust start
    where
        start v
            | B.length v >= chunkSize = C.yield [v] >> bsConcatTo chunkSize
            | otherwise = continue [v] (B.length v)
        continue chunks s = C.await >>= \case
            Nothing -> C.yield chunks
            Just v
                | B.length v + s > chunkSize -> C.yield chunks >> start v
                | otherwise -> continue (v:chunks) (s + B.length v)

untilNothing :: forall m i. (Monad m) => C.Conduit (Maybe i) m i
untilNothing = C.await >>= \case
    Just (Just val) -> do
        C.yield val
        untilNothing
    _ -> return ()

-- | A simple sink which performs gzip compression in a separate thread and
-- writes the results to `h`.
--
-- See also 'asyncGzipToFile'
asyncGzipTo :: forall m. (MonadIO m, MonadBaseControl IO m) => Handle -> C.Sink B.ByteString m ()
asyncGzipTo h = do
    let drain q = liftIO . C.runConduit $
                CA.sourceTBQueue q
                    .| untilNothing
                    .| CL.map (B.concat . reverse)
                    .| CZ.gzip
                    .| C.sinkHandle h
    bsConcatTo ((2 :: Int) ^ (15 :: Int))
        .| CA.drainTo 8 drain

-- | Compresses the output and writes to the given file with compression being
-- performed in a separate thread.
--
-- See also 'asyncGzipTo'
asyncGzipToFile :: forall m. (MonadResource m, MonadBaseControl IO m) => FilePath -> C.Sink B.ByteString m ()
asyncGzipToFile fname = C.bracketP
    (openFile fname WriteMode)
    hClose
    asyncGzipTo

-- | A source which produces the ungzipped content from the the given handle.
-- Note that this "reads ahead" so if you do not use all the input, the Handle
-- will probably be left at an undefined position in the file.
--
-- See also 'asyncGzipFromFile'
asyncGzipFrom :: forall m. (MonadIO m, MonadResource m, MonadBaseControl IO m) => Handle -> C.Source m B.ByteString
asyncGzipFrom h = do
    let prod q = liftIO $ do
                    C.runConduit $
                        C.sourceHandle h
                            .| CZ.multiple CZ.ungzip
                            .| CL.map Just
                            .| CA.sinkTBQueue q
                    atomically (TQ.writeTBQueue q Nothing)
    (CA.gatherFrom 8 prod .| untilNothing)
        `C.catchC`
        (\(e :: SZ.ZlibException) -> liftIO . ioError $ mkIOError userErrorType ("Error reading gzip file: "++displayException e) (Just h) Nothing)

-- | Open and read a gzip file with the uncompression being performed in a
-- separate thread.
--
-- See also 'asyncGzipFrom'
asyncGzipFromFile :: forall m. (MonadResource m, MonadBaseControl IO m) => FilePath -> C.Source m B.ByteString
asyncGzipFromFile fname = C.bracketP
    (openFile fname ReadMode)
    hClose
    asyncGzipFrom

-- | A simple sink which performs bzip2 compression in a separate thread and
-- writes the results to `h`.
--
-- See also 'asyncGzipToFile'
asyncBzip2To :: forall m. (MonadIO m, MonadResource m, MonadBaseControl IO m) => Handle -> C.Sink B.ByteString m ()
asyncBzip2To h = do
    let drain q = C.runConduit $
                CA.sourceTBQueue q
                    .| untilNothing
                    .| CL.map (B.concat . reverse)
                    .| CZ.bzip2
                    .| C.sinkHandle h
    bsConcatTo ((2 :: Int) ^ (15 :: Int))
        .| CA.drainTo 8 drain

-- | Compresses the output and writes to the given file with compression being
-- performed in a separate thread.
--
-- See also 'asyncGzipTo'
asyncBzip2ToFile :: forall m. (MonadResource m, MonadBaseControl IO m) => FilePath -> C.Sink B.ByteString m ()
asyncBzip2ToFile fname = C.bracketP
    (openFile fname WriteMode)
    hClose
    asyncBzip2To

-- | A source which produces the bzipped2 content from the the given handle.
-- Note that this "reads ahead" so if you do not use all the input, the Handle
-- will probably be left at an undefined position in the file.
--
-- See also 'asyncGzipFromFile'
asyncBzip2From :: forall m. (MonadIO m, MonadResource m, MonadBaseControl IO m) => Handle -> C.Source m B.ByteString
asyncBzip2From h = do
    let prod q = do
                    C.runConduit $
                        C.sourceHandle h
                            .| CZ.multiple CZ.bunzip2
                            .| CL.map Just
                            .| CA.sinkTBQueue q
                    liftIO $ atomically (TQ.writeTBQueue q Nothing)
    CA.gatherFrom 8 prod .| untilNothing

-- | Open and read a bzip2 file with the uncompression being performed in a
-- separate thread.
--
-- See also 'asyncGzipFrom'
asyncBzip2FromFile :: forall m. (MonadResource m, MonadBaseControl IO m) => FilePath -> C.Source m B.ByteString
asyncBzip2FromFile fname = C.bracketP
    (openFile fname ReadMode)
    hClose
    asyncBzip2From

-- | A simple sink which performs lzma/xz compression in a separate thread and
-- writes the results to `h`.
--
-- See also 'asyncGzipToFile'
asyncXzTo :: forall m. (MonadIO m, MonadResource m, MonadBaseControl IO m) => Handle -> C.Sink B.ByteString m ()
asyncXzTo h = do
    let drain q = C.runConduit $
                CA.sourceTBQueue q
                    .| untilNothing
                    .| CL.map (B.concat . reverse)
                    .| CX.compress Nothing
                    .| C.sinkHandle h
    bsConcatTo ((2 :: Int) ^ (15 :: Int))
        .| CA.drainTo 8 drain

-- | Compresses the output and writes to the given file with compression being
-- performed in a separate thread.
--
-- See also 'asyncGzipTo'
asyncXzToFile :: forall m. (MonadResource m, MonadBaseControl IO m) => FilePath -> C.Sink B.ByteString m ()
asyncXzToFile fname = C.bracketP
    (openFile fname WriteMode)
    hClose
    asyncXzTo

-- | A source which produces the unxzipped content from the the given handle.
-- Note that this "reads ahead" so if you do not use all the input, the Handle
-- will probably be left at an undefined position in the file.
--
-- See also 'asyncGzipFromFile'
asyncXzFrom :: forall m. (MonadIO m, MonadResource m, MonadBaseControl IO m) => Handle -> C.Source m B.ByteString
asyncXzFrom h = do
    let oneGBmembuffer = Just $ 1024 ^ (3 :: Integer)
        prod q = do
                    C.runConduit $
                        C.sourceHandle h
                            .| CZ.multiple (CX.decompress oneGBmembuffer)
                            .| CL.map Just
                            .| CA.sinkTBQueue q
                    liftIO $ atomically (TQ.writeTBQueue q Nothing)
    CA.gatherFrom 8 prod .| untilNothing


-- | Open and read a lzma/xz file with the uncompression being performed in a
-- separate thread.
--
-- See also 'asyncXzFrom'
asyncXzFromFile :: forall m. (MonadResource m, MonadBaseControl IO m) => FilePath -> C.Source m B.ByteString
asyncXzFromFile fname = C.bracketP
    (openFile fname ReadMode)
    hClose
    asyncXzFrom

-- | If the filename indicates a gzipped file (or, on Unix, also a bz2 file),
-- then it reads it and uncompresses it.
--
-- On Windows, attempting to read from a bzip2 file, results in 'error'.
--
-- For the case of gzip, 'asyncGzipFromFile' is used.
conduitPossiblyCompressedFile :: (MonadBaseControl IO m, MonadResource m) => FilePath -> C.Source m B.ByteString
conduitPossiblyCompressedFile fname
    | ".gz" `isSuffixOf` fname = asyncGzipFromFile fname
    | ".xz" `isSuffixOf` fname = asyncXzFromFile fname
    | ".bz2" `isSuffixOf` fname = asyncBzip2FromFile fname
    | otherwise = C.sourceFile fname

conduitPossiblyCompressedToFile :: (MonadBaseControl IO m, MonadResource m) => FilePath -> C.Sink B.ByteString m ()
conduitPossiblyCompressedToFile fname
    | ".gz" `isSuffixOf` fname = asyncGzipToFile fname
    | ".xz" `isSuffixOf` fname = asyncXzToFile fname
    | ".bz2" `isSuffixOf` fname = asyncBzip2ToFile fname
    | otherwise = C.sinkFile fname
