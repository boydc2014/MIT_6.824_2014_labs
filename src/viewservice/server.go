
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  curView View  
  nextView View  
  lastPingTime map[string]time.Time
  primaryAcked bool
}


func (vs *ViewServer) tryProceedView() {
  fmt.Printf("try to change view\n")
  if vs.primaryAcked {
    fmt.Printf("new view: %s %s\n", vs.nextView.Primary, vs.nextView.Backup)
    vs.curView = vs.nextView
    vs.primaryAcked = false
  } else {
    fmt.Printf("view unchanged\n")
  }
}

func (vs *ViewServer) prepareNextView() {
  vs.nextView = vs.curView
  vs.nextView.Viewnum += 1
}

func (vs *ViewServer) tryChangeViewTo(primary string, backup string) {
  vs.prepareNextView()
  vs.nextView.Primary = primary
  vs.nextView.Backup = backup
  vs.tryProceedView()
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()

  vs.lastPingTime[args.Me] = time.Now()

  if args.Viewnum == 0 {
    if args.Me == vs.curView.Primary ||
       args.Me == vs.curView.Backup {
      // known server, must be restared
      if args.Me == vs.curView.Primary {
        // Primary restarted, promote Backup to Primary
        vs.tryChangeViewTo(vs.curView.Backup, "")
      } else {
        // Backup restarted, do nothing
      }
       
    } else {
      // new server
      fmt.Printf("new server %s come\n", args.Me)
      if vs.curView.Primary == "" {
        // take as Primary
        fmt.Printf("try to take as Primary\n")
        vs.tryChangeViewTo(args.Me, "")
      } else if vs.curView.Backup == "" {
        // take as Backup
        fmt.Printf("try to take as Backup\n")
        vs.tryChangeViewTo(vs.curView.Primary, args.Me)
      } else {
        fmt.Printf("do nothing for new server\n")
      }
    }
  } else {
    // Ping with non-zero Viewnum
    if args.Me == vs.curView.Primary &&
       args.Viewnum == vs.curView.Viewnum {
      vs.primaryAcked = true
    }
  }

  // always reply with the current View
  reply.View = vs.curView

  vs.mu.Unlock()

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  vs.mu.Lock()
  reply.View = vs.curView
  vs.mu.Unlock()

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
  
  // check primary
  if vs.curView.Primary != "" && 
     time.Since(vs.lastPingTime[vs.curView.Primary])  > DeadPings*PingInterval {
    // primary down, prompt backup
    fmt.Printf("primary down, prompt backup\n")
    vs.tryChangeViewTo(vs.curView.Backup, "")
  }

  if vs.curView.Backup != "" &&
     time.Since(vs.lastPingTime[vs.curView.Backup]) > DeadPings*PingInterval {
    // backup down, delete backup
    fmt.Printf("backup down, delete backup\n")
    vs.tryChangeViewTo(vs.curView.Primary, "")
  }

 

  vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.

  vs.mu = sync.Mutex{}
  vs.curView = View{}
  vs.nextView = View{}
  vs.lastPingTime = make(map[string]time.Time)
  vs.primaryAcked = true

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
