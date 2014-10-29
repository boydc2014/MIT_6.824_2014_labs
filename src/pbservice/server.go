package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  store map[string]string

  processed map[int64]bool
  results map[int64]string

  mu *sync.Mutex

  view viewservice.View
}

// used when Primary sync data with Backup
func (pb *PBServer) PutAll(args *PutAllArgs, reply *PutAllReply) error {
  if (pb.view.Backup != pb.me) {
    return nil
  }
  fmt.Printf("PutAll received at %s\ndata:%s\n%s\n%s\n", pb.me, args.Store, args.Processed, args.Results)

  pb.mu.Lock()
  pb.store = args.Store
  pb.processed = args.Processed
  pb.results = args.Results
  reply.Err = OK
  pb.mu.Unlock()

  return nil
}

// Handle the forwarded Put from Primary to Backup
func (pb *PBServer) SubPut(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()

  fmt.Printf("sub put %s received at %s\n", args, pb.me)
  
  if pb.view.Backup != pb.me {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return nil
  }

  // filter duplicated requests
  if pb.processed[args.Id] {
    reply.Err = OK
    pb.mu.Unlock()
    return nil
  }

  //pb.processed[args.Id] = true
  pb.doPut(args, reply)
  
  reply.Err = OK
  pb.mu.Unlock()
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  DPrintf("put %s received at %s\n", args, pb.me)
  
  // ensure I'm primary
  if pb.view.Primary != pb.me {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return nil
  }

  // filter duplicated requests
  if pb.processed[args.Id] {
    if args.DoHash {
      reply.PreviousValue = pb.results[args.Id]
    }
    reply.Err = OK
    pb.mu.Unlock()
    return nil
  }


  // forward to backup first to avoid inconsistency
  if pb.view.Backup != "" {
    //subArgs := &SubPutArgs{args.Key, args.Value, args.DoHash}
    //subReply := &SubPutReply{} 
    success := false
    for !success && pb.view.Backup != ""{
      call(pb.view.Backup, "PBServer.SubPut", args, reply)
      if reply.Err == OK {
        success = true
      } else if reply.Err == ErrWrongServer {
        fmt.Printf("WARN: forward put to wrong server\n")
        reply.Err = ErrWrongServer
        pb.mu.Unlock()
        return nil
      } else {
        // retry
        time.Sleep(time.Second)
        fmt.Printf("WARN: forward %s to backup %s fails, retry\n", args, pb.view.Backup)
      }
    }
  }

  // if we mark the request as processed too early
  // that will cause the new backup think it has been processed 
  // pb.processed[args.Id] = true  // marked as processed

  pb.doPut(args, reply)

  reply.Err = OK
  pb.mu.Unlock()
  return nil
}

func (pb *PBServer) doPut(args *PutArgs, reply *PutReply) {
  pb.processed[args.Id] = true
  if (args.DoHash) {
    reply.PreviousValue = pb.store[args.Key]
    pb.results[args.Id] = pb.store[args.Key]
    pb.store[args.Key] = strconv.Itoa(int(hash(pb.store[args.Key]+args.Value)))
  } else {
    pb.store[args.Key] = args.Value
  }
}

func (pb *PBServer) SubGet(args *GetArgs, reply *GetReply) error {
  if pb.view.Backup != pb.me {
    fmt.Printf("Wrong Server in SubGet at %s\n", pb.me)
    reply.Err = ErrWrongServer
    return nil
  }

  reply.Err = OK
  return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  if (pb.view.Primary != pb.me) {
    fmt.Printf("Wrong Server in Get\n")
    reply.Err = ErrWrongServer
    return nil
  }

  // forward to backup to confirm I'm primary
  if pb.view.Backup != "" {
    success := false
    for !success && pb.view.Backup != ""{
      call(pb.view.Backup, "PBServer.SubGet", args, reply)
      if reply.Err == OK {
        success = true
      } else if reply.Err == ErrWrongServer {
        fmt.Printf("WARN: forward put to wrong server\n")
        reply.Err = ErrWrongServer
        return nil
      } else {
        // retry
        time.Sleep(time.Second)
        fmt.Printf("WARN: forward %s to backup %s fails, retry\n", args, pb.view.Backup)
      }
    }
  }


  pb.mu.Lock()
  reply.Err = OK
  reply.Value = pb.store[args.Key]
  pb.mu.Unlock()
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {

  newview, err := pb.vs.Ping(pb.view.Viewnum)
  if err != nil {
    fmt.Printf("view serivice unreachable!, error %s\n", err)
    return;
  }

  if pb.view.Viewnum != newview.Viewnum {
    fmt.Printf("view changed from \n%s ==>\n%s\n",pb.view, newview)
    if (pb.view.Primary == pb.me &&
        newview.Primary == pb.me &&
        pb.view.Backup != newview.Backup &&
        newview.Backup != "") {
      // backup changes and I'm still primary 
      pb.view = newview
      fmt.Printf("new backup is %s\n", newview.Backup)
      pb.syncWithBackup()
    }
  }
  // only when pb is in the view, proceed to new view
  if pb.me == newview.Primary || pb.me == newview.Backup {
    pb.view = newview
  } else {
    fmt.Printf("I'm not in the view, keep trying\n")
  }
}

func (pb *PBServer) syncWithBackup() {
  // should we retry here?
  success := false
  args := &PutAllArgs{pb.store, pb.processed, pb.results}
  reply := &PutAllReply{}
  for !success {
    call(pb.view.Backup, "PBServer.PutAll", args, reply)
    if reply.Err != OK {
      fmt.Printf("WARN: put all fails, retry\n")
      time.Sleep(time.Second)
    } else {
      success = true
    }
  }
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.store = make(map[string]string)
  pb.processed = make(map[int64]bool)
  pb.results = make(map[int64]string)
  pb.mu = &sync.Mutex{}

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
