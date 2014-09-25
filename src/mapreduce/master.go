package mapreduce

import "container/list"
import "fmt"

import "sync"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  busy bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func TryDispatchWorker(mr *MapReduce, requests []chan string) []chan string {
  if len(requests) == 0 {
    // no requests, no need to dispatch
    return requests
  }

  for worker := range mr.Workers {
    // try to find an unbusy worker
    if !mr.Workers[worker].busy {
      mr.Workers[worker].busy = true
      request := requests[0]
      request <- worker
      return requests[1:]
    }
  }
  return requests
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here

  workerGet := make(chan chan string)
  workerPut := make(chan string)
  // worker management
  //   handle worker registration
  //   handle worker request from job managment routine
  go func() {
    queue := []chan string{}
    for {
      select {
      case newWorkerName := <-mr.registerChannel:
        fmt.Printf("New worker registered: %s\n", newWorkerName)
        workerInfo := &WorkerInfo{}
        workerInfo.address = newWorkerName
        workerInfo.busy = false
        mr.Workers[newWorkerName] = workerInfo
        queue = TryDispatchWorker(mr, queue)
      case workerRequest := <-workerGet:
        fmt.Printf("Worker request get!\n")
        queue = append(queue, workerRequest)
        queue = TryDispatchWorker(mr, queue)
      case putWorkerName := <-workerPut:
        fmt.Printf("Worker returned: %s\n", putWorkerName)
        mr.Workers[putWorkerName].busy = false
        queue = TryDispatchWorker(mr, queue)
      }
    }
  }()

  // job management
  //   assgin Map jobs
  //   wait for Map jobs to finsish
  //   assgin Reduce jobs
  //   wait for Reduce jobs to finish

  // assgin map jobs
  mapWaitGroup := sync.WaitGroup{}
  mapWaitGroup.Add(mr.nMap)
  for mapJobId := 0; mapJobId < mr.nMap; mapJobId++ {
    go func(mapJobId int) {
      workerRequest := make(chan string)
      workerGet <- workerRequest    // send request
      workerName := <-workerRequest // wait for response
      fmt.Printf("worker get: %s for map job %d\n", workerName, mapJobId)
      mapJob := CreateDoJobArgs(mr.file, Map, mapJobId, mr.nReduce)
      reply := &DoJobReply{}
      call(workerName, "Worker.DoJob", mapJob, reply)
      if reply.OK {
        fmt.Printf("Map job %d done successfully!\n", mapJobId)
      }
      workerPut <- workerName
      mapWaitGroup.Done()
    }(mapJobId)
  }

  // wait for map jobs to finish
  mapWaitGroup.Wait()

  // assgin reduce jobs
  reduceWaitGroup := sync.WaitGroup{}
  reduceWaitGroup.Add(mr.nReduce)
  for reduceJobId := 0; reduceJobId < mr.nReduce; reduceJobId++ {
    go func(reduceJobId int) {
      workerRequest := make(chan string)
      workerGet <- workerRequest
      workerName := <- workerRequest
      fmt.Printf("worker get: %s for reduce job %d\n", workerName, reduceJobId)
      reduceJob := CreateDoJobArgs(mr.file, Reduce, reduceJobId, mr.nMap)
      reply := &DoJobReply{}
      call(workerName, "Worker.DoJob", reduceJob, reply)
      if reply.OK {
        fmt.Printf("Reduce job %d done successfully!\n", reduceJobId)
      }
      workerPut <- workerName
      reduceWaitGroup.Done()
    }(reduceJobId)
  }

  // wait for reduce jobs to finish
  reduceWaitGroup.Wait()

  return mr.KillWorkers()
}

func CreateDoJobArgs(file string, operation JobType, jobNumber int, numOtherPhase int) *DoJobArgs {
  args := &DoJobArgs{}
  args.File = file
  args.Operation = operation
  args.JobNumber = jobNumber
  args.NumOtherPhase = numOtherPhase
  return args
}
