package mme

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"tinyepc/rpcs"
)

type mme struct {
	// TODO: Implement this!
	conn                     *rpc.Client
	listener                 net.Listener
	myHostPort, loadBalancer string
	numServed                int
	replicas                 []string
	state                    map[uint64]rpcs.MMEState
	stateLock, numServedLock *sync.Mutex
}

var LOGF *log.Logger

// New creates and returns (but does not start) a new MME.
func New() MME {
	// TODO: Implement this!
	var m *mme
	m = new(mme)
	return m
}

func (m *mme) Close() {
	// TODO: Implement this!
	m.conn.Close()
	m.listener.Close()
}

func (m *mme) StartMME(hostPort string, loadBalancer string) error {
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	// TODO: Implement this!

	m.myHostPort = hostPort
	m.loadBalancer = loadBalancer
	m.numServed = 0
	m.replicas = make([]string, 0)
	m.state = make(map[uint64]rpcs.MMEState)
	m.stateLock, m.numServedLock = &sync.Mutex{}, &sync.Mutex{}
	m.conn, err = rpc.DialHTTP("tcp", "localhost"+loadBalancer)
	if err != nil {
		return err
	}

	m.listener, err = net.Listen("tcp", "localhost"+hostPort)
	if err != nil {
		return err
	}
	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.WrapMME(m))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(m.listener, nil)

	var ja *rpcs.JoinArgs = new(rpcs.JoinArgs)
	var jr *rpcs.JoinReply = new(rpcs.JoinReply)
	ja.MMEport = m.myHostPort
	m.conn.Call("LoadBalancer.RecvJoin", ja, jr)

	return nil
}

func (m *mme) RecvUERequest(args *rpcs.UERequestArgs, reply *rpcs.UERequestReply) error {
	// TODO: Implement this!
	m.numServedLock.Lock()
	m.numServed++
	m.numServedLock.Unlock()
	var tempStruct rpcs.MMEState
	m.stateLock.Lock()
	if _, ok := m.state[args.UserID]; !ok {
		tempStruct.Balance = 100
		m.state[args.UserID] = tempStruct
	}
	if args.UEOperation == rpcs.Call {
		tempStruct.Balance = m.state[args.UserID].Balance - 5
	} else if args.UEOperation == rpcs.SMS {
		tempStruct.Balance = m.state[args.UserID].Balance - 1
	} else if args.UEOperation == rpcs.Load {
		tempStruct.Balance = m.state[args.UserID].Balance + 10
	}
	m.state[args.UserID] = tempStruct
	m.stateLock.Unlock()
	// return errors.New("RecvUERequest() not implemented")
	return nil
}

// RecvMMEStats is called by the tests to fetch MME state information
// To pass the tests, please follow the guidelines below carefully.
//
// <reply> (type *rpcs.MMEStatsReply) fields must be set as follows:
// 		Replicas: 	List of hostPort strings of replicas
// 					example: [":4110", ":1234"]
// 		NumServed: 	Number of user requests served by this MME
// 					example: 5000
// 		State: 		Map of user states with hash of UserID as key and rpcs.MMEState as value
//					example: 	{
//								"3549791233": {"Balance": 563, ...},
//								"4545544485": {"Balance": 875, ...},
//								"3549791233": {"Balance": 300, ...},
//								...
//								}
func (m *mme) RecvMMEStats(args *rpcs.MMEStatsArgs, reply *rpcs.MMEStatsReply) error {
	// TODO: Implement this!
	reply.NumServed = m.numServed
	reply.Replicas = m.replicas
	reply.State = m.state
	return nil
}

// TODO: add additional methods/functions below!
