package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"

import "unicode"
import "strings"
import "strconv"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
    // split into words
    f := func(c rune) bool {
        return !unicode.IsLetter(c) && !unicode.IsNumber(c)
    }
    words := strings.FieldsFunc(value, f)

    // count word
    counts := make(map[string]int)
    for _ , word := range words {
        counts[word] +=  1
    }

    // form output
    kvList := list.New()
    for word, count := range counts {
        kv := mapreduce.KeyValue{word, strconv.Itoa(count)}
        kvList.PushBack(kv)
    }
    return kvList
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
    countTotal := 0
    for item := values.Front(); item != nil; item = item.Next() {
        count, _ := strconv.Atoi(item.Value.(string))
        countTotal += count
    }
    return strconv.Itoa(countTotal)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])    
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
