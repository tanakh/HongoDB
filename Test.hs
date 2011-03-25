{-# Language OverloadedStrings #-}

import Database.HongoDB

import Control.Monad.Trans
import Data.Enumerator as E
import Data.Enumerator.List as EL
import System.IO

main :: IO ()
main = do
  runHashDB $ do
    set "a" "b"
    set "c" "d"
    v <- get "a"
    liftIO $ print v
    
    e <- enum
    let f = do
          mkv <- EL.head
          case mkv of
            Just (k, v) -> liftIO (print (k, v)) >> f
            Nothing -> return ()
    run_ $ e $$ f
  
  return ()
