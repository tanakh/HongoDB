{-# Language ScopedTypeVariables, MultiParamTypeClasses, OverloadedStrings, GeneralizedNewtypeDeriving, TupleSections #-}

module Database.HongoDB.HashFile (
  HashFile,
  HashFileState,
  openHashFile, openHashFile',
  closeHashFile,
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
import Data.Enumerator as E
import Data.Enumerator.List as EL
import Data.Hashable
import Data.Int
import Data.IORef
import Data.Monoid
import System.Directory

import Prelude as P hiding (lookup)

magicString :: B.ByteString
magicString = "HHDB"

formatVersion :: (Int8, Int8)
formatVersion = (0, 0)

defaultBucketSize :: Int
defaultBucketSize = 1024

newtype HashFile m a =
  HashFile { unHashFile :: ReaderT HashFileState m a }
  deriving (Monad, MonadIO, MonadTrans, Functor, Applicative, MonadControlIO)

instance Monad m => MonadReader HashFileState (HashFile m) where
  ask = HashFile ask
  local f m = HashFile $ local f (unHashFile m)

data HashFileState =
  HashFileState
  { file   :: IORef F.File
  , filename :: FilePath
  , header :: IORef Header
  , lock :: MVar ()
  }

askHeader :: MonadIO m => HashFile m Header
askHeader =
  liftIO . readIORef =<< asks header

putHeader :: MonadIO m => Header -> HashFile m ()
putHeader h =
  liftIO . flip writeIORef h =<< asks header

modifyHeader :: MonadIO m => (Header -> Header) -> HashFile m ()
modifyHeader mf = do
  stat <- ask
  liftIO $ modifyIORef (header stat) mf

askFile :: MonadIO m => HashFile m F.File
askFile =
  liftIO . readIORef =<< asks file

putFile :: MonadIO m => F.File -> HashFile m ()
putFile f =
  liftIO . flip writeIORef f =<< asks file

openHashFile :: FilePath -> IO HashFileState
openHashFile = openHashFile' defaultBucketSize

openHashFile' :: Int -> FilePath -> IO HashFileState
openHashFile' bsize path = do
  b <- doesFileExist path
  f <- F.open path
  unless b $
    initHashFile f bsize
  fr <- newIORef f
  hr <- newIORef =<< readHeader f
  
  l  <- newMVar ()
  return $ HashFileState
    { file = fr
    , filename = path
    , header = hr
    , lock = l
    }

closeHashFile :: HashFileState -> IO ()
closeHashFile stat = do
  f <- readIORef $ file stat
  h <- readIORef $ header stat
  writeHeader f h

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

headerSize :: Int
headerSize =
  B.length $ fromHeader emptyHeader

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

fromInt48le :: Int -> B.ByteString
fromInt48le = BB.toByteString . BB.fromWrite . writeInt48le

toInt48le :: B.ByteString -> Int
toInt48le bs =
  case A.parse anyWord48le bs of
    A.Done _ r ->
      r
    _ ->
      error "toInt48le: fail"

nextPrime :: Int -> Int
nextPrime n = P.head $ P.filter isPrime [n..] where
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

initialHeader :: Int -> Header
initialHeader bsize =
  emptyHeader
  { bucketSize = bsize
  , freeBlockSize = fbsize
  , fileSize = fsize
  , bucketStart = bstart
  , freeBlockStart = fstart
  , recordStart = rstart
  }
  where
    fbsize = 64
    bstart = headerSize
    fstart = bstart + bsize * 6
    rstart = fstart + fbsize * 6
    fsize = rstart

initHashFile :: F.File -> Int -> IO ()
initHashFile f bsize = do
  let h = initialHeader (nextPrime bsize)
  F.clear f
  writeHeader f h
  F.write f (B.replicate (bucketSize h * 6) 0xff) (bucketStart h)
  F.write f (B.replicate (freeBlockSize h * 6) 0xff) (freeBlockStart h)

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

emptyEntry :: Int
emptyEntry = 0xffffffffffff

-- TODO: optimize
sizeRecord :: Record -> Int
sizeRecord = B.length . fromRecord

fromRecord :: Record -> B.ByteString
fromRecord r = BB.toByteString $ BB.fromWrite $
  writeInt48le (rnext r) `mappend`
  writeVInt (B.length $ rkey r) `mappend`
  writeVInt (B.length $ rval r) `mappend`
  BB.writeByteString (rkey r) `mappend`
  BB.writeByteString (rval r)

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

readPartialRecord :: MonadIO m => Int -> HashFile m (Record, Bool)
readPartialRecord ofs = do
  f <- askFile
  h <- askHeader
  bs <- liftIO $ F.read f 64 (recordStart h + ofs)
  case A.parse parseRecord bs of
    A.Done _ r -> return (r, True)
    A.Partial _ -> case A.parse parseRecordHeader bs of
      A.Done _ r -> return (r, False)
      _ -> error "readPartial: failed"
    _ -> error "readPartial: failed"

readCompleteRecord :: MonadIO m => Int -> Record -> HashFile m Record
readCompleteRecord ofs r = do
  let rsize = sizeRecord r
  f <- askFile
  h <- askHeader
  bs <- liftIO $ F.read f rsize (recordStart h + ofs)
  case A.parse parseRecord bs of
    A.Done _ v -> return v
    _ -> error "readComplete: failed"

readCompleteRecord' :: MonadIO m => Int -> HashFile m Record
readCompleteRecord' ofs = do
  (pr, whole) <- readPartialRecord ofs
  if whole
    then return pr
    else readCompleteRecord ofs pr

lookupFreeBlock :: MonadIO m => Int -> HashFile m (Maybe Int)
lookupFreeBlock size = do
  f <- askFile
  h <- askHeader
  locs <- liftIO $ F.read f 6 (freeBlockStart h + ix * 6)
  let loc = toInt48le locs
  if loc == emptyEntry
    then do
    return Nothing
    else do
    next <- liftIO $ F.read f 6 (recordStart h + loc)
    liftIO $ F.write f next (freeBlockStart h + ix * 6)
    return $ Just loc
  where
    ix = exponent (fromIntegral size :: Double)

addFreeBlock :: MonadIO m => Int -> Record -> HashFile m ()
addFreeBlock ofs r = do
  f <- askFile
  h <- askHeader
  liftIO $ do
    bef <- F.read f 6 (freeBlockStart h + ix * 6)
    F.write f bef (recordStart h + ofs)
    F.write f (fromInt48le ofs) (freeBlockStart h + ix * 6)
  where
    ix = max 0 (exponent (fromIntegral $ sizeRecord r :: Double) - 1)

addRecord :: MonadIO m => Record -> HashFile m Int
addRecord r = do
  h <- askHeader
  mbloc <- lookupFreeBlock (sizeRecord r)
  let st = recordStart h
      end = fileSize h
  case mbloc of
    Nothing -> do
      let ofs = end - st
      nofs <- writeRecord ofs r
      putHeader $ h { fileSize = st + nofs }
      return ofs
    Just ofs -> do
      _ <- writeRecord ofs r
      return ofs

writeRecord :: MonadIO m => Int -> Record -> HashFile m Int
writeRecord ofs r = do
  f <- askFile
  h <- askHeader
  let bs = fromRecord r
  liftIO $ F.write f bs (recordStart h + ofs)
  return $ ofs + B.length bs

writeNext :: MonadIO m => Int -> Int -> HashFile m ()
writeNext ofs next = do
  f <- askFile
  h <- askHeader
  liftIO $ F.write
    f
    (BB.toByteString $ BB.fromWrite $ writeInt48le next)
    (recordStart h + ofs)

readBucket :: (Functor m, MonadIO m) => Int -> HashFile m Int
readBucket bix = do
  bofs <- bucketStart <$> askHeader
  h <- askHeader
  f <- askFile
  bs <- liftIO $ F.read f 6 (bofs + bix * 6)
  return $ toInt48le bs

writeBucket :: (MonadIO m) => Int -> Int -> HashFile m ()
writeBucket bix val = do
  f <- askFile
  h <- askHeader
  liftIO $ F.write
    f
    (BB.toByteString $ BB.fromWrite $ writeInt48le val)
    (bucketStart h + bix * 6)

lookup :: (Functor m, MonadIO m) =>
          B.ByteString -> HashFile m (Maybe B.ByteString)
lookup key = do
  mb <- lookup' key
  return $ rval . fst <$> mb

lookup' :: (Functor m, MonadIO m) =>
           B.ByteString -> HashFile m (Maybe (Record, (Int, Int)))
lookup' key = do
  sz <- bucketSize <$> askHeader
  let ha = hash key
  let bix = ha `mod` sz
  link <- readBucket bix
  findLink emptyEntry link

  where
    findLink bef cur
      | cur == emptyEntry =
        return Nothing
      | otherwise = do
        -- TODO: FIXME: when read less than key length
        (r, whole) <- readPartialRecord cur
        if rkey r == key
          then
          if whole
            then Just <$> return (r, (bef, cur))
            else Just . (, (bef, cur)) <$> readCompleteRecord cur r
          else findLink cur (rnext r)

insert :: (Functor m, MonadControlIO m) => B.ByteString -> B.ByteString -> HashFile m ()
insert key val = do
  sz <- bucketSize <$> askHeader
  let ha = hash key
  let bix = ha `mod` sz
  let nr = Record emptyEntry key val
  toplink <- readBucket bix
  mbv <- lookup' key
  case mbv of
    Nothing -> do
      nhead <- addRecord nr
      writeBucket bix nhead
      incRecordSize
    Just (r, (bef, cur)) -> do
      let curSize = sizeRecord r
          newSize = sizeRecord nr
      -- If current size is larger than new size and
      -- new size is larger than half of current size,
      -- then just replace it.
      if curSize >= newSize && curSize <= newSize * 2
        then do
        -- replace
        _ <- writeRecord cur (r { rval = val })
        return ()
        else do
        -- remove and add
        -- 1. rewrite before's link
        when (bef /= emptyEntry) $
          -- if current record has parent
          writeNext bef (rnext r)
        -- 2. alloc new record
        let nlink = if bef /= emptyEntry then toplink else rnext r
        nhead <- addRecord (nr { rnext = nlink })
        -- 3. rewrite bucket's link
        writeBucket bix nhead
        -- 4. add current to free pool
        addFreeBlock cur r
        return ()
  checkCapacity

maxBucketRatio :: Double
maxBucketRatio = 0.9

checkCapacity :: (Functor m, MonadControlIO m) => HashFile m ()
checkCapacity = do
  h <- askHeader
  let ratio = fromIntegral (recordSize h) /
              fromIntegral (bucketSize h)
  when (ratio >= maxBucketRatio) $
    doubleBucket

doubleBucket :: forall m . (Functor m, MonadControlIO m) => HashFile m ()
doubleBucket = do
  h <- askHeader
  
  name <- asks filename
  let tmpName = name ++ ".tmp"
  
  f <- liftIO $ openHashFile' (bucketSize h * 2) tmpName
  e <- H.enum
  -- TODO: may be not efficient
  run_ $ e $$ go f
  
  liftIO $ closeHashFile f
  liftIO . closeHashFile =<< ask
  
  liftIO $ renameFile tmpName name
  
  nf <- liftIO $ openHashFile name
  s <- ask
  liftIO $ writeIORef (file s) =<< readIORef (file nf)
  liftIO $ writeIORef (header s) =<< readIORef (header nf)
  
  where
    go f = do
      mkv <- EL.head
      case mkv of
        Just (key, val) -> do
          lift $ runHashFile f $ H.set key val
          go f
        Nothing -> do
          return ()

remove :: (Functor m, MonadIO m) => B.ByteString -> HashFile m ()
remove key = do
  sz <- bucketSize <$> askHeader
  let ha = hash key
  let bix = ha `mod` sz
  mbv <- lookup' key
  case mbv of
    Nothing ->
      return ()
    Just (r, (bef, cur)) -> do
      if bef /= emptyEntry
        then do
        writeNext bef (rnext r)
        else do
        writeBucket bix (rnext r)
      addFreeBlock cur r
      decRecordSize

incRecordSize :: MonadIO m => HashFile m ()
incRecordSize = modifyHeader (\h -> h { recordSize = recordSize h + 1 })

decRecordSize :: MonadIO m => HashFile m ()
decRecordSize = modifyHeader (\h -> h { recordSize = recordSize h - 1 })

--

instance (Functor m, MonadControlIO m) => H.DB (HashFile m) where
  accept key f = withLock $ do
    mval <- lookup key
    (act, r) <- f mval
    case act of
      H.Replace val ->
        insert key val
      H.Remove ->
        remove key
      H.Nop ->
        return ()
    return r
  {-# INLINABLE accept #-}
  
  count = withLock $ do
    recordSize <$> askHeader
  {-# INLINABLE count #-}
  
  clear = withLock $ do
    f <- askFile
    liftIO $ initHashFile f defaultBucketSize
    putHeader =<< liftIO (readHeader f)
  {-# INLINABLE clear #-}

  enum = return go where
    go step = do
      stat <- lift $ HashFile ask
      h <- liftIO $ readIORef (header stat)
      go' 0 (bucketSize h) stat step
    
    go' bix bsize stat step@(E.Continue f)
      | bix >= bsize =
        E.returnI step
      | otherwise = do
        pos <- lift $ readBucket bix
        if pos /= emptyEntry
          then do
          kvs <- lift $ readLink pos
          f (E.Chunks kvs) E.>>== go' (bix+1) bsize stat
          else
          go' (bix+1) bsize stat step

    go' _ _ _ step =
      E.returnI step
    
readLink :: MonadIO m => Int -> HashFile m [(B.ByteString, B.ByteString)]
readLink pos
  | pos == emptyEntry = return []
  | otherwise = do
    r  <- readCompleteRecord' pos
    rs <- readLink (rnext r)
    return $ (rkey r, rval r) : rs

withLock :: MonadControlIO m => HashFile m a -> HashFile m a
withLock m = do
  l <- HashFile (asks lock)
  MC.bracket
    (liftIO $ takeMVar l)
    (liftIO . putMVar l)
    (const m)

runHashFile :: MonadControlIO m => HashFileState -> HashFile m a -> m a
runHashFile stat db = do
  runReaderT (unHashFile db) stat
