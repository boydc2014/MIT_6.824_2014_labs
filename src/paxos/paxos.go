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
  dones []int
  me int // index into peers[]
  states map[int]*State
  MinSeq int

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

    for px.CanStart(seq) {

      // Phase 1: Prepare
      fmt.Printf("[peer:%d seq:%d] prepare Start with value %v\n", px.me, seq, v)
      n := px.genProposalNum()
      px.mu.Lock()
      pArgs := &PrepareArgs{ Seq:seq, Num:n, Done:px.dones[px.me] }
      px.mu.Unlock()
      pReply := &PrepareReply{ }

      prepared := 0 // the number of prepared acceptors
      maxAcceptedNum := 0
      maxAcceptedVal := v // initial with the start value
      for id, peer := range px.peers {

        if px.CallPrepare(peer, pArgs, pReply) && pReply.OK {
          prepared++
          if pReply.Na > maxAcceptedNum {
            maxAcceptedNum = pReply.Na
            maxAcceptedVal = pReply.Va
          }
          fmt.Printf("[peer:%d seq:%d] prepare OK from (%d)%s reply %+v\n", px.me, seq, id, peer, pReply)
        } else {
          fmt.Printf("[peer:%d seq:%d] prepare fails from (%d)%s\n", px.me, seq, id, peer)
        }

        px.UpdateDone(pReply.Done, id)
      }
      if prepared < majority {
        fmt.Printf("[peer:%d seq:%d] ERROR: prepare phase fails, go to error retry!\n", px.me, seq)
        time.Sleep(time.Millisecond * 100)
        continue
      }

      //fmt.Printf("[%d]INFO: prepare consult result, max accepted (%d, %s)\n", px.me, maxAcceptedNum, maxAcceptedVal) 

      // we can not issue our value if we've seen accepted value
      v = maxAcceptedVal

      // Phase 2: Accept
      fmt.Printf("[peer:%d seq:%d] accept Start with value %v\n", px.me, seq, v)
      
      accepted := 0
      px.mu.Lock()
      aArgs := &AcceptArgs{ Seq:seq, Na:n, Va:v, Done:px.dones[px.me] }
      fmt.Printf("[peer:%d seq:%d] accept Args sent: %+v\n", px.me, seq, aArgs.Na)
      px.mu.Unlock()
      aReply := &AcceptReply{ OK:false }
      for id, peer := range px.peers {
        if px.CallAccept(peer, aArgs, aReply) && aReply.OK {
          accepted++
          fmt.Printf("[peer:%d seq:%d] accept OK from (%d)%s with reply %+v\n", px.me, seq, id, peer, aReply);
        } else {
          fmt.Printf("[peer:%d seq:%d] WARN: accept fails from (%d)%s\n", px.me, seq, id, peer);
        }

        px.UpdateDone(aReply.Done, id)
      }
      
      if accepted < majority {
        fmt.Printf("[peer:%d seq:%d] ERROR: accept phase fails, go to error retry\n", px.me, seq)
        time.Sleep(time.Millisecond * 100)
        continue
      }
      
      fmt.Printf("[peer:%d seq:%d] INFO: accept result %d accpeted\n", px.me, seq, accepted)

      // Phase 3: Decide
  
      fmt.Printf("[peer:%d seq:%d] decide Start\n", px.me, seq)
      
      // we do not care decide fails or not, because a value has been decided
      // we can learn it later even all decide messages were lost 
      px.mu.Lock()
      dArgs := &DecideArgs{ Seq:seq, Va:v, Done:px.dones[px.me] }
      px.mu.Unlock()
      dReply := &DecideReply{ OK:false }
      nDecided := 0
      for id, peer := range px.peers {
        if px.CallDecide(peer, dArgs, dReply) && dReply.OK {
          nDecided++
          fmt.Printf("[peer:%d seq:%d] decide OK from (%d)%s with reply %+v\n", px.me, seq, id, peer, dReply)
        } else {
          fmt.Printf("[peer:%d seq:%d] WARN: decide fails from (%d)%s\n", px.me, seq, id, peer)
        }
        px.UpdateDone(dReply.Done, id)
      }

      // failure-retry after a certain time
      //if !px.IsDecided(seq) {
      //  time.Sleep(time.Millisecond * 100);
      //}

    }

    fmt.Printf("[peer:%d seq:%d] Round finished\n", px.me, seq)
    
  }()
}

func (px *Paxos) CanStart(seq int) bool {
  px.mu.Lock()
  defer px.mu.Unlock()

  // check Min first
  if seq < px.MinSeq {
    fmt.Printf("[peer:%d] start fail seq %d < min %d\n", px.me, seq, px.MinSeq)
    return false
  }
  
  // check decided directly from px instead of Status()
  // avoid creating a new state
  if _, ok := px.states[seq]; ok && px.states[seq].Decided {
    return false
  }
  
  // otherwise it can start
  return true
}

func (px *Paxos) IsDecided(seq int) bool {
  decided, _ := px.Status(seq)
  return decided
}

// util 


// create a state to track a paxos instance with seq num: seq
// must called with px.mu Lock
func (px *Paxos) createStateIfNeed(seq int) {
  if _, ok := px.states[seq]; !ok {
    fmt.Printf("[peer:%d seq:%d] create state \n", px.me, seq)
    px.states[seq] = new(State)
  }
}

func (px *Paxos) genProposalNum() int {
  tmp := (int(time.Now().UnixNano())/int(time.Millisecond)) * 10 + px.me;
  //fmt.Printf("[%d]INFO: unique proposal num generated: %d\n", px.me, tmp)
  return tmp
}


type PrepareArgs struct {
  Seq int
  Num int
  Done int
}

type PrepareReply struct {
  Na int
  Va interface {}
  OK bool
  Done int
}


func (px *Paxos) IsMe(peer string) bool{
  return px.peers[px.me] == peer
}
// caller of prepare, use function call instead of rpc locally
func (px *Paxos) CallPrepare(peer string, args *PrepareArgs, reply *PrepareReply) bool {
  if px.IsMe(peer) {
    //fmt.Printf("call prepare locally\n")
    px.Prepare(args, reply)
    return true
  }
  return call(peer, "Paxos.Prepare", args, reply)
}

func (px *Paxos) CallAccept(peer string, args *AcceptArgs, reply *AcceptReply) bool {
  if px.IsMe(peer) {
    px.Accept(args, reply)
    return true
  }
  return call(peer, "Paxos.Accept", args, reply)
}

func (px *Paxos) CallDecide(peer string, args *DecideArgs, reply *DecideReply) bool {
  if px.IsMe(peer) {
    px.Decide(args, reply)
    return true
  }
  return call(peer, "Paxos.Decide", args, reply)
}
// the prepare handler for an acceptor
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Seq < px.MinSeq {
    fmt.Println("[peer:%d seq:%d] prepare fail seq %d < min %d\n", px.me, args.Seq, args.Seq, px.MinSeq)
    reply.OK = false
    reply.Done = px.dones[px.me]
    return nil
  }

  px.createStateIfNeed(args.Seq)

  state := px.states[args.Seq]
  if args.Num > state.Np {
    state.Np = args.Num
    reply.Na = state.Na
    reply.Va = state.Va
    reply.OK = true
  } else {
    reply.OK = false
  }
  reply.Done = px.dones[px.me]
  fmt.Printf("[peer:%d seq:%d] prepare result: %+v reply: %+v state: %+v\n", px.me, args.Seq, args, reply, state)
  //if px.me == 0 {
  //  reply.Done = 100
  //}
  //fmt.Printf("[%d]INFO, dones array %d Done %d passed in Done %d pass out\n", px.me, px.dones, args.Done, reply.Done)
  return nil
}

type AcceptArgs struct {
  Seq int
  Na int
  Va interface {}
  Done int
}

type AcceptReply struct {
  Done int
  OK bool
}

// the accept handler for an acceptor
// seq is the instance id
// n is the proposal id
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Seq < px.MinSeq {
    fmt.Println("[peer:%d seq:%d] accept fail seq %d < min %d\n", px.me, args.Seq, args.Seq, px.MinSeq)
    reply.OK = false
    reply.Done = px.dones[px.me]
    return nil
  }

  fmt.Printf("[peer:%d seq:%d] accept args received: %+v\n", px.me, args.Seq, args)
  // we also need ensure state is created here besides at prepare
  // because an propose call may be lost so state will not be created
  // at prepare.
  px.createStateIfNeed(args.Seq)

  state := px.states[args.Seq]
  if args.Na >= state.Np {
    state.Np = args.Na
    state.Na = args.Na
    state.Va = args.Va
    reply.OK = true
  } else {
    reply.OK = false
  }
  reply.Done = px.dones[px.me]
  return nil
}

type DecideArgs struct {
  Seq int
  Va interface {}
  Done int
}

type DecideReply struct {
  OK bool
  Done int
}

// decide handler
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Seq < px.MinSeq {
    fmt.Printf("[peer:%d seq:%d] decide fail seq %d < min %d\n", px.me, args.Seq, args.Seq, px.MinSeq)
    reply.OK = false
    reply.Done = px.dones[px.me]
    return nil
  }

  fmt.Printf("[peer:%d] handle decide with arg %+v\n", px.me, args)
  px.createStateIfNeed(args.Seq)
  
  // we need to figure out what should an out-dated acceptor handle a 
  // new decide info. should the acceptor update 
  state := px.states[args.Seq]
  state.Va = args.Va
  state.Decided = true

  reply.OK = true
  reply.Done = px.dones[px.me]
  return nil
}



//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // update Done
  px.UpdateDone(seq, px.me)
}

// update Done value for a specified peer 
func (px *Paxos) UpdateDone(seq int, id int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  if seq < 0 {
    return
  }
  fmt.Printf("[peer:%d] update, peer %d done %d\n", px.me, id, seq)
  px.dones[id] = seq
  // triger a drop here
  min := px.dones[0]
  for _, done := range px.dones {
    if done < min {
      min = done
    }
  }
  if min < 0 {
    // can not update Min since there is some done value = -1
    px.MinSeq = -1
  } else {
    px.MinSeq = min+1
    fmt.Printf("[peer:%d] update Min %d\n", px.me, px.MinSeq)
  }

  // drop the values 
  toDrop := make([]int, 0)
  for seq, _ := range px.states {
    if seq < px.MinSeq {
      toDrop = append(toDrop, seq)
    }
  }
  if len(toDrop) > 0 {
    fmt.Printf("[peer:%d] seqs need to be droped %v\n", px.me, toDrop)
  }

  for _, seq := range toDrop {
    delete(px.states, seq)
  }
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
  px.mu.Lock()
  defer px.mu.Unlock()
  if px.MinSeq < 0 {
    return 0
  }
  return px.MinSeq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  fmt.Printf("[peer:%d seq:%d] check status\n", px.me, seq)
  px.createStateIfNeed(seq);

  
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
  px.dones = make([]int, len(px.peers))
  for index, _ := range px.dones {
    px.dones[index] = -1 // init to -1 for all peers
  }
  px.me = me
  px.states = make(map[int]*State)
  px.MinSeq = -1


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
