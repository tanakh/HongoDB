module Database.HongoDB.Base (
  DB(..),
  Action(..),
  ) where

import qualified Data.ByteString.Char8 as B
import Data.Maybe

data Action
  = Replace B.ByteString
  | Remove
  | Nop

class (Monad m) => DB m where
  accept :: B.ByteString ->
            (Maybe B.ByteString -> m (Action, a)) ->
            m a
  
  get :: B.ByteString -> m (Maybe B.ByteString)
  get key = accept key $ \r -> return (Nop, r)
  
  set :: B.ByteString -> B.ByteString -> m ()
  set key val = accept key $ \_ -> return (Replace val, ())
  
  add :: B.ByteString -> B.ByteString -> m Bool
  add key val = accept key $ f where
    f Nothing = return (Replace val, True)
    f _ = return (Nop, False)
  
  remove :: B.ByteString -> m Bool
  remove key = accept key $ \m -> return (Remove, isJust m)

  count :: m Int
  clear :: m ()

  -- enum :: m (E.Enumerator B.ByteString m a)
