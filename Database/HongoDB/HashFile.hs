module Database.HongoDB.HashFile (
  ) where

import Database.HongoDB.Base
import qualified Database.HongoDB.Internal.File as F

newtype HashFile m a =
  HashFile { unHashFile :: ReaderT File m a }
  deriving (Monad, MonadIO, MonadTrans, Functor, Applicative)

openHashFile :: FilePath -> IO HashFile
openHashFile path = do
  b <- doesFileExists path
  if b
    then do
    f <- F.open path
    initHashFile f
    return f
    else do
    F.open path

initHashFile :: File -> IO ()
initHashFIle f = do
  

--instance DB (HashFile m) where
--  accept :: 
