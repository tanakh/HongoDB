{-# Language GeneralizedNewtypeDeriving #-}

module Database.HongoDB.HashMem (
  HashMem,
  runHashMem,
  ) where

import Database.HongoDB.Base

import Control.Applicative
import Control.Monad.IO.Control
import Control.Monad.Reader
import qualified Data.ByteString.Char8 as B
import qualified Data.Enumerator as E
import qualified Data.HashMap.Strict as HM
import Data.IORef

type Table = HM.HashMap B.ByteString B.ByteString

newtype HashMem m a =
  HashMem { unHashMem :: ReaderT (IORef Table) m a }
  deriving (Monad, MonadIO, MonadTrans, Functor, Applicative, MonadControlIO)

instance (MonadControlIO m) => DB (HashMem m) where
  accept key f = modifyTable $ \t -> do
    (act, r) <- f (HM.lookup key t)
    let nt = case act of
          Replace val -> HM.insert key val t
          Remove -> HM.delete key t
          Nop -> t
    return (nt, r)
  
  count = withTable $ \t ->
    return $ HM.size t

  clear = modifyTable $ \_ ->
    return (HM.empty, ())
  
  enum = withTable $ \t ->
    return $ go (HM.toList t)
    where
      go [] (E.Continue k) = E.continue k
      go xs (E.Continue k) =
        let (as, bs) = splitAt 256 xs in
        k (E.Chunks as) E.>>== go bs
      go _ step = E.returnI step

withTable :: MonadIO m => (Table -> HashMem m a) -> HashMem m a
withTable f = do
  r <- HashMem ask
  f =<< liftIO (readIORef r)

modifyTable :: MonadIO m => (Table -> HashMem m (Table, a)) -> HashMem m a
modifyTable f = do
  r <- HashMem ask
  (t, v) <- f =<< liftIO (readIORef r)
  liftIO $ writeIORef r t
  return v

runHashMem :: MonadIO m => HashMem m a -> m a
runHashMem hdb = do
  ref <- liftIO $ newIORef HM.empty
  runReaderT (unHashMem hdb) ref
