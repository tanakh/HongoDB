{-# Language OverloadedStrings #-}

import Database.HongoDB

import qualified Data.ByteString as B
import Control.Monad
import Control.Monad.Trans
import Data.Enumerator as E
import Data.Enumerator.List as EL
import System.Directory
import System.IO

{-
main :: IO ()
main = do
  runHashMem $ do
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
-}

fname = "tmp.hgdb"

main :: IO ()
main = do
  e <- doesFileExist fname
  when e $ removeFile fname
  f <- openHashFile fname
  runHashFile f $ do
    liftIO . print =<< count
    set "a" "b"
    liftIO . print =<< count
    set "c" "d"
    liftIO . print =<< count
    liftIO . print =<< get "a"
    liftIO . print =<< get "b"
    liftIO . print =<< get "c"
    set "a" "x"
    liftIO . print =<< count
    liftIO . print =<< get "a"
    set "a" "xyz"
    liftIO . print =<< count
    liftIO . print =<< get "a"
    liftIO . print =<< get "b"
    liftIO . print =<< get "c"
    remove "a"
    liftIO . print =<< count
    liftIO . print =<< get "a"
    remove "c"
    liftIO . print =<< count
  
  runHashFile f $ do
    set "a" "a"
    set "b" "b"
    set "c" "c"

  runHashFile f $ do
    liftIO . print =<< count
    liftIO . print =<< get "c"
    clear
    liftIO . print =<< count
    liftIO . print =<< get "c"

  closeHashFile f
  
  return ()
