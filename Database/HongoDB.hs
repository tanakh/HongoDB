{-# Language GeneralizedNewtypeDeriving #-}
{-# Language FlexibleContexts #-}

module Database.HongoDB (
  DB(..),
  
  HashDB,
  runHashDB,
  ) where

import Control.Applicative
import Control.Monad.Trans
import Control.Monad.Reader
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import Data.IORef

class (Monad m) => DB m where
  get :: B.ByteString -> m (Maybe B.ByteString)
  set :: B.ByteString -> B.ByteString -> m ()
  add :: B.ByteString -> B.ByteString -> m Bool
  remove :: B.ByteString -> m Bool

  count :: m Int
  clear :: m ()

  -- enum :: m (E.Enumerator B.ByteString m a)

type Table = HM.HashMap B.ByteString B.ByteString

newtype HashDB m a =
  HashDB { unHashDB :: ReaderT (IORef Table) m a }
  deriving (Monad, MonadIO, MonadTrans, Functor, Applicative)

instance (MonadIO m) => DB (HashDB m) where
  get key = withTable $ \t -> do
    return $ HM.lookup key t
  
  set key val = modifyTable $ \t -> do
    return $ HM.insert key val t
  
  add key val = do
    mb <- get key
    case mb of
      Just _ ->
        return False
      Nothing -> do
        modifyTable $ \t -> do
          return $ HM.insert key val t
        return True
  
  remove key = return False
  
  count = withTable $ \t -> do
    return $ HM.size t

  clear = return ()

withTable :: MonadIO m => (Table -> HashDB m a) -> HashDB m a
withTable f = do
  r <- HashDB ask
  f =<< (liftIO $ readIORef r)

modifyTable :: MonadIO m => (Table -> HashDB m Table) -> HashDB m ()
modifyTable f = do
  r <- HashDB ask
  t <- f =<< (liftIO  $ readIORef r)
  liftIO $ writeIORef r t

runHashDB :: MonadIO m => HashDB m a -> m a
runHashDB hdb = do
  ref <- liftIO $ newIORef HM.empty
  runReaderT (unHashDB hdb) ref
