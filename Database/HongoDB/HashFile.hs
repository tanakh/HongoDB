{-# Language OverloadedStrings, GeneralizedNewtypeDeriving, TupleSections #-}

module Database.HongoDB.HashFile (
  HashFile,
  openHashFile, closeHashFile,
  runHashFile,
  ) where

import qualified Database.HongoDB.Base as H
import qualified Database.HongoDB.Internal.File as F

import qualified Blaze.ByteString.Builder as BB
import Control.Applicative
import Control.Concurrent.MVar
import qualified Control.Exception.Control as MC
import Control.Monad.IO.Control
import Control.Monad.Trans
import Control.Monad.Reader
import qualified Data.Attoparsec as A
import qualified Data.Attoparsec.Binary as A
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.Enumerator as E
import Data.Hashable
import Data.Int
import Data.IORef
import Data.Monoid
import System.Directory

import Prelude hiding (lookup)

magicString :: B.ByteString
magicString = "HHDB"

formatVersion :: (Int8, Int8)
formatVersion = (0, 0)

newtype HashFile m a =
  HashFile { unHashFile :: ReaderT HashFileState m a }
  deriving (Monad, MonadIO, MonadTrans, Functor, Applicative, MonadControlIO)

data HashFileState =
  HashFileState
  { file   :: F.File
  , header :: IORef Header
  , lock :: MVar ()
  }

modifyHeader :: (Header -> Header) -> HashFileState -> IO ()
modifyHeader mf stat = do
  modifyIORef (header stat) mf

openHashFile :: FilePath -> IO F.File
openHashFile path = do
  b <- doesFileExist path
  f <- F.open path
  unless b $
    initHashFile f
  return f

closeHashFile :: F.File -> IO ()
closeHashFile = F.close

data Header =
  Header
  { magic          :: B.ByteString
  , version        :: (Int8, Int8)
  , bucketSize     :: Int
  , recordSize     :: Int
  , freeBlockSize  :: Int
  , fileSize       :: Int
  , bucketStart    :: Int
  , freeBlockStart :: Int
  , recordStart    :: Int
  }
  deriving (Show)

fromHeader :: Header -> B.ByteString
fromHeader h = BB.toByteString $ BB.fromWrite $
  BB.writeByteString (magic h) `mappend`
  BB.writeInt8 (fst $ version h) `mappend`
  BB.writeInt8 (snd $ version h) `mappend`
  writeInt48le (bucketSize h) `mappend`
  writeInt48le (recordSize h) `mappend`
  writeInt48le (freeBlockSize h) `mappend`
  writeInt48le (fileSize h) `mappend`
  writeInt48le (bucketStart h) `mappend`
  writeInt48le (freeBlockStart h) `mappend`
  writeInt48le (recordStart h)

writeInt48le :: Int -> BB.Write
writeInt48le n =
  BB.writeInt32le (fromIntegral n) `mappend`
  BB.writeInt16le (fromIntegral $ n `shiftR` 32)

writeVInt :: Int -> BB.Write
writeVInt 0 = BB.writeWord8 0
writeVInt n
  | n < 128 =
    BB.writeWord8 (fromIntegral n)
  | otherwise =
    BB.writeWord8 (fromIntegral $ n `mod` 128) `mappend`
    writeVInt (n `div` 128)

-- TODO: optimize
-- vlen :: Int -> Int
-- vlen = B.length . BB.toByteString . BB.fromWrite . writeVInt

parseHeader :: A.Parser Header
parseHeader =
  Header
  <$> A.take 4
  <*> ((,)
       <$> (fromIntegral <$> A.anyWord8)
       <*> (fromIntegral <$> A.anyWord8))
  <*> anyWord48le
  <*> anyWord48le
  <*> anyWord48le
  <*> anyWord48le
  <*> anyWord48le
  <*> anyWord48le
  <*> anyWord48le

anyWord48le :: A.Parser Int
anyWord48le = do
  a <- fromIntegral <$> A.anyWord32le
  b <- fromIntegral <$> A.anyWord16le
  return $ a .|. (b `shiftL` 32)

anyVInt :: A.Parser Int
anyVInt = do
  n <- A.anyWord8
  if n < 128
    then return (fromIntegral n)
    else do
    r <- anyVInt
    return $ (fromIntegral n .&. 0x7f) + r * 128

toHeader :: B.ByteString -> Header
toHeader bs =
  case A.parse parseHeader bs of
    A.Done _ r ->
      r
    _ ->
      error "toHeader: no header"

toInt48le :: B.ByteString -> Int
toInt48le bs =
  case A.parse anyWord48le bs of
    A.Done _ r ->
      r
    _ ->
      error "toInt48le: fail"

nextPrime :: Int -> Int
nextPrime n = head $ filter isPrime [n..] where
  isPrime a = and [ a`mod`i /= 0
                  | i <- [2 .. floor (sqrt (fromIntegral a) :: Double)]]

emptyHeader :: Header
emptyHeader =
  Header
  { magic = magicString
  , version = formatVersion
  , bucketSize = 0
  , recordSize = 0
  , freeBlockSize = 0
  , fileSize = 0
  , bucketStart = 0
  , freeBlockStart = 0
  , recordStart = 0
  }

initialHeader :: Header
initialHeader =
  emptyHeader
  { bucketSize = bsize
  , freeBlockSize = fbsize
  , fileSize = fsize
  , bucketStart = bstart
  , freeBlockStart = fstart
  , recordStart = rstart
  }
  where
    bsize = nextPrime 1024
    fbsize = 64
    bstart = headerSize
    fstart = bstart + bsize * 6
    rstart = fstart + fbsize * 6
    fsize = rstart

headerSize :: Int
headerSize =
  B.length $ fromHeader emptyHeader

initHashFile :: F.File -> IO ()
initHashFile f = do
  let h = initialHeader
  F.clear f
  writeHeader f h
  F.write f (B.replicate (bucketSize h * 6) 0xff) (bucketStart h)
  F.write f (B.replicate (freeBlockSize h * 6) 0xff) (freeBlockStart h)

emptyEntry :: Int
emptyEntry = 0xffffffffffff

readHeader :: F.File -> IO Header
readHeader f =
  toHeader <$> F.read f headerSize 0

writeHeader :: F.File -> Header -> IO ()
writeHeader f h =
  F.write f (fromHeader h) 0

--

data Record =
  Record
  { rnext :: Int
  , rkey  :: B.ByteString
  , rval  :: B.ByteString
  }
  deriving (Show)

fromRecord :: Record -> B.ByteString
fromRecord r = BB.toByteString $ BB.fromWrite $
  writeInt48le (rnext r) `mappend`
  writeVInt (B.length $ rkey r) `mappend`
  writeVInt (B.length $ rval r) `mappend`
  BB.writeByteString (rkey r) `mappend`
  BB.writeByteString (rval r)

-- TODO: optimize
parseRecord :: A.Parser Record
parseRecord = do
  rn <- anyWord48le
  klen <- anyVInt
  vlen <- anyVInt
  Record rn <$> A.take klen <*> A.take vlen

parseRecordHeader :: A.Parser Record
parseRecordHeader = do
  rn <- anyWord48le
  klen <- anyVInt
  vlen <- anyVInt
  return $ Record rn (B.replicate klen 0) (B.replicate vlen 0)

readPartialRecord :: Int -> HashFileState -> IO (Record, Bool)
readPartialRecord ofs HashFileState { file = f, header = hr } = do
  h <- readIORef hr
  bs <- F.read f 64 (recordStart h + ofs)
  case A.parse parseRecord bs of
    A.Done _ r -> return (r, True)
    A.Partial _ -> case A.parse parseRecordHeader bs of
      A.Done _ r -> return (r, False)
      _ -> error "readPartial: failed"
    _ -> error "readPartial: failed"

readCompleteRecord :: Int -> Record -> HashFileState -> IO Record
readCompleteRecord ofs r HashFileState { file = f, header = hr } = do
  let rsize = sizeRecord r
  h <- readIORef hr
  bs <- F.read f rsize (recordStart h + ofs)
  case A.parse parseRecord bs of
    A.Done _ v -> return v
    _ -> error "readComplete: failed"

readCompleteRecord' :: Int -> HashFileState -> IO Record
readCompleteRecord' ofs stat = do
  (pr, whole) <- readPartialRecord ofs stat
  if whole
    then return pr
    else readCompleteRecord ofs pr stat

addRecord :: Record -> HashFileState -> IO Int
addRecord r stat @ (HashFileState { header = hr }) = do
  -- TODO: first search free pool
  h <- readIORef hr
  let st = recordStart h
      end = fileSize h
  nend <- writeRecord (end - st) r stat
  writeIORef hr $ h { fileSize = st + nend }
  return (end - st)

writeRecord :: Int -> Record -> HashFileState -> IO Int
writeRecord ofs r stat = do
  h <- readIORef (header stat)
  let bs = fromRecord r
  F.write (file stat) bs (recordStart h + ofs)
  return $ ofs + B.length bs

-- TODO: optimize
sizeRecord :: Record -> Int
sizeRecord = B.length . fromRecord

writeNext :: Int -> Int -> HashFileState -> IO ()
writeNext ofs next stat = do
  st <- fileSize <$> readIORef (header stat)
  F.write (file stat) (BB.toByteString $ BB.fromWrite $ writeInt48le next) (st + ofs)

readBucket :: Int -> HashFileState -> IO Int
readBucket bix stat = do
  bofs <- ((+ bix * 6) . bucketStart <$> readIORef (header stat))
  bs <- F.read (file stat) 6 bofs
  return $ toInt48le bs

writeBucket :: Int -> Int -> HashFileState -> IO ()
writeBucket bix val stat =
  F.write
  (file stat)
  (BB.toByteString $ BB.fromWrite $ writeInt48le val)
  =<< ((+ bix * 6) . bucketStart <$> readIORef (header stat))

lookup :: B.ByteString -> HashFileState -> IO (Maybe B.ByteString)
lookup key stat = do
  mb <- lookup' key stat
  return $ rval . fst <$> mb

lookup' :: B.ByteString -> HashFileState ->
           IO (Maybe (Record, (Int, Int)))
lookup' key stat = do
  sz <- bucketSize <$> readIORef (header stat)
  let ha = hash key
  let bix = ha `mod` sz
  link <- readBucket bix stat
  findLink emptyEntry link

  where
    findLink bef cur
      | cur == emptyEntry =
        return Nothing
      | otherwise = do
        -- TODO: FIXME: when read less than key length
        (r, whole) <- readPartialRecord cur stat
        if rkey r == key
          then
          if whole
            then Just <$> return (r, (bef, cur))
            else Just . (, (bef, cur)) <$> readCompleteRecord cur r stat
          else findLink cur (rnext r)

insert :: B.ByteString -> B.ByteString -> HashFileState -> IO ()
insert key val stat = do
  sz <- bucketSize <$> readIORef (header stat)
  let ha = hash key
  let bix = ha `mod` sz
  let nr = Record emptyEntry key val
  toplink <- readBucket bix stat
  mbv <- lookup' key stat
  case mbv of
    Nothing -> do
      nhead <- addRecord nr stat
      writeBucket bix nhead stat
      incRecordSize stat
    Just (r, (bef, cur)) -> do
      let curSize = sizeRecord r
          newSize = sizeRecord nr
      -- If current size is larger than new size and
      -- new size is larger than half of current size,
      -- then just replace it.
      if curSize >= newSize && curSize <= newSize * 2
        then do
        -- replace
        _ <- writeRecord cur (r { rval = val }) stat
        return ()
        else do
        -- remove and add
        -- 1. rewrite before's link
        when (bef /= emptyEntry) $
          -- if current record has parent
          writeNext bef (rnext r) stat
        -- 2. alloc new record
        let nlink = if bef /= emptyEntry then toplink else rnext r
        nhead <- addRecord (nr { rnext = nlink }) stat
        -- 3. rewrite bucket's link
        writeBucket bix nhead stat
        -- 4. add current to free pool
        -- TODO
        return ()

remove :: B.ByteString -> HashFileState -> IO ()
remove key stat = do
  sz <- bucketSize <$> readIORef (header stat)
  let ha = hash key
  let bix = ha `mod` sz
  mbv <- lookup' key stat
  case mbv of
    Nothing ->
      return ()
    Just (r, (bef, _)) -> do
      if bef /= emptyEntry
        then do
        writeNext bef (rnext r) stat
        else do
        writeBucket bix (rnext r) stat
      -- TODO: add current to free pool
      decRecordSize stat
      return ()

incRecordSize :: HashFileState -> IO ()
incRecordSize = modifyHeader (\h -> h { recordSize = recordSize h + 1 })

decRecordSize :: HashFileState -> IO ()
decRecordSize = modifyHeader (\h -> h { recordSize = recordSize h - 1 })

--

instance MonadControlIO m => H.DB (HashFile m) where
  accept key f = withState $ \stat -> do
    mval <- liftIO $ lookup key stat
    (act, r) <- f mval
    case act of
      H.Replace val ->
        liftIO $ insert key val stat
      H.Remove ->
        liftIO $ remove key stat
      H.Nop ->
        return ()
    return r
  
  count = withState $ \stat -> do
    h <- liftIO $ readIORef $ header stat
    return $ recordSize h
  
  clear = withState $ \stat -> do
    liftIO $ initHashFile (file stat)
    h <- liftIO $ readHeader (file stat)
    liftIO $ writeIORef (header stat) h
    
  enum = return go where
    go step = do
      stat <- lift $ HashFile ask
      h <- liftIO $ readIORef (header stat)
      liftIO $ print h
      go' 0 (bucketSize h) stat step
    
    go' bix bsize stat step@(E.Continue f)
      | bix >= bsize =
        E.returnI step
      | otherwise = do
        pos <- liftIO $ readBucket bix stat
        if pos /= emptyEntry
          then do
          kvs <- liftIO $ readLink pos stat
          f (E.Chunks kvs) E.>>== go' (bix+1) bsize stat
          else
          go' (bix+1) bsize stat step

    go' _ _ _ step =
      E.returnI step
    
readLink :: Int -> HashFileState -> IO [(B.ByteString, B.ByteString)]
readLink pos stat
  | pos == emptyEntry = return []
  | otherwise = do
    r  <- readCompleteRecord' pos stat
    rs <- readLink (rnext r) stat
    return $ (rkey r, rval r) : rs

withState :: MonadControlIO m => (HashFileState -> HashFile m a) -> HashFile m a
withState f = do
  l <- HashFile (asks lock)
  MC.bracket
    (liftIO $ takeMVar l)
    (liftIO . putMVar l)
    (\_ -> f =<< HashFile ask)

runHashFile :: MonadControlIO m => F.File -> HashFile m a -> m a
runHashFile f db = do
  h <- liftIO $ readHeader f
  r <- liftIO $ HashFileState f <$> newIORef h <*> newMVar ()
  v <- runReaderT (unHashFile db) r
  nh <- liftIO $ readIORef (header r)
  liftIO $ writeHeader f nh
  return v
