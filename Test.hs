{-# Language OverloadedStrings #-}

import Control.Monad.Trans
import Database.HongoDB
import System.IO

main :: IO ()
main = do
  runHashDB $ do
    set "a" "b"
    set "c" "d"
    v <- get "a"
    liftIO $ print v
  
  return ()
