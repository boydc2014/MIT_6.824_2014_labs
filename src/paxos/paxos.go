package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"


type State struct {
  Np int
  Na int
  Va interface{}
  Decided bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  states map[int]*State

  // Your data here.
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  // using a go routine to return immediately
  go func() {
    // while not finished
    //  generate a proposal number
    //  call prepare
    //  if prepared from a majority
    //    call accept
    //    if accepted from a majority
    //      call decide
    
    majority := len(px.peers)/2 + 1

    for decided, _ := px.Status(seq); !decided; time.Sleep(time.Millisecond * 100) {
      // wait for some time before retry to avoid keep preparing

      // Phase 1: Prepare
      n := px.genProposalNum()
      pArgs := &PrepareArgs{ Seq:seq, Num:n }
      pReply := &PrepareReply{ OK:false }

      prepared := 0 // the number of prepared acceptors
      maxAcceptedNum := 0
      maxAcceptedVal := v // initial with the start value
      for _, peer := range px.peers {
        if call(peer, "Paxos.Prepare", pArgs, pReply) && pReply.OK {
          prepared++
          if pReply.Na > maxAcceptedNum {
            maxAcceptedVal = pReply.Va
          }
          fmt.Printf("INFO: prepare OK from %s with reply %s\n", peer, pReply)
        } else {
          fmt.Printf("WARN: prepare fails from %s\n", peer)
        }
      }
      if prepared < majority {
        fmt.Printf("ERROR: prepare phase fails, go to error retry!\n")
        continue
      }

      fmt.Printf("INFO: prepare consult result, max accepted (%d, %s)\n", maxAcceptedNum, maxAcceptedVal) 

      // we can not issue our value if we've seen accepted value
      v = maxAcceptedVal

      // Phase 2: Accept
      
      accepted := 0
      aArgs := &AcceptArgs{ Seq:seq, Na:n, Va:v }
      aReply := &AcceptReply{ OK:false }
      for _, peer := range px.peers {
        if call(peer, "Paxos.Accept", aArgs, aReply) && aReply.OK {
          accepted++
          fmt.Printf("INFO: accept OK from %s\n", peer);
        } else {
          fmt.Printf("WARN: accept fails from %s\n", peer);
        }
      }
      
      if accepted < majority {
        fmt.Printf("ERROR: accept phase fails, go to error retry\n")
        continue
      }
      
      fmt.Printf("INFO: accept result %d accpeted\n", accepted)
      // Phase 3: Decide
      
      // we do not care decide fails or not, because a value has been decided
      // we can learn it later even all decide messages were lost 
      //px.states[seq].Decided = true
      dArgs := &DecideArgs{ Seq:seq, Va:v}
      dReply := &DecideReply{ OK:false }
      nDecided := 0
      for _, peer := range px.peers {
        if call(peer, "Paxos.Decide", dArgs, dReply) && dReply.OK {
          nDecided++
          fmt.Printf("INFO: decide OK from %s\n", peer)
        } else {
          fmt.Printf("WARN: decide fails from %s\n", peer)
        }
      }

//error:
      //fmt.Printf("Wait for a certain time before retry\n")
      //time.Sleep(time.Millisecond * 100)
    }

    fmt.Printf("Round finished Seq %s!\n", seq)
    
  }()
}

// util 
func (px *Paxos) createStateIfNeed(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  if _, ok := px.states[seq]; !ok {
    fmt.Printf("INFO: instance(%d) state does not exist, create\n", seq)
    px.states[seq] = new(State)
  }
}

func (px *Paxos) genProposalNum() int {
  tmp := (int(time.Now().UnixNano())/int(time.Millisecond)) * 10 + px.me;
  fmt.Printf("INFO: unique proposal num generated: %d\n", tmp)
  return tmp
}


type PrepareArgs struct {
  Seq int
  Num int
}

type PrepareReply struct {
  Na int
  Va interface {}
  OK bool
}

// the prepare handler for an acceptor
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.createStateIfNeed(args.Seq)

  px.mu.Lock()
  defer px.mu.Unlock()

  state := px.states[args.Seq]
  if args.Num > state.Np {
    state.Np = args.Num
    reply.Na = state.Na
    reply.Va = state.Va
    reply.OK = true
  } else {
    reply.OK = false
  }
  return nil
}

type AcceptArgs struct {
  Seq int
  Na int
  Va interface {}
}

type AcceptReply struct {
  OK bool
}

// the accept handler for an acceptor
// seq is the instance id
// n is the proposal id
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  // we also need ensure state is created here besides at prepare
  // because an propose call may be lost so state will not be created
  // at prepare.
  px.createStateIfNeed(args.Seq)

  px.mu.Lock()
  defer px.mu.Unlock()

  state := px.states[args.Seq]
  if args.Na >= state.Np {
    state.Np = args.Na
    state.Na = args.Na
    state.Va = args.Va
    reply.OK = true
  } else {
    reply.OK = false
  }
  return nil
}

type DecideArgs struct {
  Seq int
  Va interface {}
}

type DecideReply struct {
  OK bool
}

// decide handler
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.createStateIfNeed(args.Seq)

  px.mu.Lock()
  defer px.mu.Unlock()
  
  // we need to figure out what should an out-dated acceptor handle a 
  // new decide info. should the acceptor update 
  state := px.states[args.Seq]
  state.Va = args.Va
  state.Decided = true

  reply.OK = true
  return nil
}



//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  
  max := -1
  for seq, _ := range px.states {
    if seq > max {
      max = seq
    }
  }

  return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  fmt.Printf("INFO: check status for %d at %s\n", seq, px.me)
  px.createStateIfNeed(seq);
  

  px.mu.Lock()
  defer px.mu.Unlock()
  
  decided := px.states[seq].Decided
  value := px.states[seq].Va
  if !decided {
    value = nil
  }
  
  return decided, value
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.states = make(map[int]*State)


  // Your initialization code here.

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
