{-# Language GeneralizedNewtypeDeriving #-}

module Database.HongoDB.HashMem (
  HashDB,
  runHashDB,
  ) where

import Database.HongoDB.Base

import Control.Applicative
import Control.Monad.Reader
import Control.Monad.Trans
import qualified Data.ByteString.Char8 as B
import qualified Data.Enumerator as E
import qualified Data.HashMap.Strict as HM
import Data.IORef

type Table = HM.HashMap B.ByteString B.ByteString

newtype HashDB m a =
  HashDB { unHashDB :: ReaderT (IORef Table) m a }
  deriving (Monad, MonadIO, MonadTrans, Functor, Applicative)

instance (MonadIO m) => DB (HashDB m) where
  accept key f = modifyTable $ \t -> do
    (act, r) <- f (HM.lookup key t)
    let nt = case act of
          Replace val -> HM.insert key val t
          Remove -> HM.delete key t
          Nop -> t
    return (nt, r)
  
  count = withTable $ \t -> do
    return $ HM.size t

  clear = modifyTable $ \_ ->
    return (HM.empty, ())
  
  enum = withTable $ \t -> do 
    return $ go (HM.toList t)
    where
      go [] (E.Continue k) = E.continue k
      go xs (E.Continue k) =
        let (as, bs) = splitAt 256 xs in
        k (E.Chunks as) E.>>== go bs
      go _ step = E.returnI step

withTable :: MonadIO m => (Table -> HashDB m a) -> HashDB m a
withTable f = do
  r <- HashDB ask
  f =<< (liftIO $ readIORef r)

modifyTable :: MonadIO m => (Table -> HashDB m (Table, a)) -> HashDB m a
modifyTable f = do
  r <- HashDB ask
  (t, v) <- f =<< (liftIO  $ readIORef r)
  liftIO $ writeIORef r t
  return v

runHashDB :: MonadIO m => HashDB m a -> m a
runHashDB hdb = do
  ref <- liftIO $ newIORef HM.empty
  runReaderT (unHashDB hdb) ref
