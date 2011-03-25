module Database.HongoDB.Internal.File (
  File,
  open, close,
  read, write,
  ) where

import Control.Applicative
import qualified Data.ByteString as B
import System.Posix.IO
import System.Posix.IO.ByteString
import System.Posix.Types

import Prelude hiding (read)

newtype File =
  File { unFile :: Fd }

initialFileMode :: FileMode
initialFileMode = 0o644

open :: FilePath -> IO File
open path =
  File <$> openFd path ReadWrite (Just initialFileMode) defaultFileFlags

close :: File -> IO ()
close (File fd) =
  closeFd fd

read :: File -> Int -> Int -> IO B.ByteString
read (File fd) cnt ofs =
  fdPread fd (fromIntegral cnt) (fromIntegral ofs) 
{-# INLINABLE read #-}

write :: File -> B.ByteString -> Int -> IO ()
write (File fd) bs ofs = do
  fdPwrite fd bs (fromIntegral ofs)
  return ()
{-# INLINABLE write #-}
