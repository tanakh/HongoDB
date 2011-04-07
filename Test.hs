{-# Language OverloadedStrings #-}

import Database.HongoDB

import Control.Monad as M
import Control.Monad.Trans
import qualified Data.ByteString.Char8 as C
import Data.Enumerator as E
import Data.Enumerator.List as EL
import System.Directory
import System.Random

main :: IO ()
main = do
  -- testHashMem
  -- testHashFile
  bench

testHashMem :: IO ()
testHashMem = do
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

fname = "tmp.hgdb"

testHashFile :: IO ()
testHashFile = do
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
    e <- enum
    let f = do
          mkv <- EL.head
          case mkv of
            Just kv -> liftIO (print ("enum", kv)) >> f
            Nothing -> return ()
    run_ $ e $$ f

  runHashFile f $ do
    liftIO . print =<< count
    liftIO . print =<< get "c"
    clear
    liftIO . print =<< count
    liftIO . print =<< get "c"

  closeHashFile f
  
  return ()

bench :: IO ()
bench = do
  print "bench"
  e <- doesFileExist fname
  when e $ removeFile fname
  f <- openHashFile fname
  runHashFile f $ do
    forM_ [1..10000] $ \i -> do
      key <- liftIO $ M.replicateM 10 (randomRIO ('a', 'z'))
      val <- liftIO $ M.replicateM 10 (randomRIO ('a', 'z'))
      liftIO $ print (i, key, val)
      set (C.pack key) (C.pack val)
  closeHashFile f
