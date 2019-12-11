// Partner 1: 2020-10-0148
// Partner 2: 2020-10-0287

package loadbalancer

import (
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"tinyepc/rpcs"
)

type loadBalancer struct {
	// TODO: Implement this!
	ringWeight, myPort, ringNodes, physicalNodes, virtualNodes int
	listener                                                   net.Listener
	hashes                                                     []uint64
	serverNames                                                []string
	hashObject                                                 *ConsistentHashing
	mmeRPCObjectMap                                            map[uint64]*rpc.Client
}

// New returns a new instance of LoadBalancer, but does not start it
func New(ringWeight int) LoadBalancer {
	// TODO: Implement this!
	var lb *loadBalancer
	lb = new(loadBalancer)
	lb.ringWeight = ringWeight
	lb.physicalNodes = 0
	lb.virtualNodes = 0
	lb.hashes = make([]uint64, 0)
	lb.serverNames = make([]string, 0)
	lb.hashObject = new(ConsistentHashing)
	lb.mmeRPCObjectMap = make(map[uint64]*rpc.Client)
	return lb
}

func (lb *loadBalancer) StartLB(port int) error {
	// TODO: Implement this!
	lb.myPort = port
	var err error = nil
	lb.listener, err = net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		return err
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.WrapLoadBalancer(lb))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(lb.listener, nil)

	return nil
}

func (lb *loadBalancer) Close() {
	// TODO: Implement this!
	lb.listener.Close()
}

func (lb *loadBalancer) RecvUERequest(args *rpcs.UERequestArgs, reply *rpcs.UERequestReply) error {
	// TODO: Implement this!
	var ra *rpcs.UERequestArgs = new(rpcs.UERequestArgs)
	var rr *rpcs.UERequestReply = new(rpcs.UERequestReply)
	tempHash := lb.hashObject.Hash(strconv.FormatUint(args.UserID, 10))
	ra.UserID = tempHash
	ra.UEOperation = args.UEOperation

	// Logic to find index taken from: https://stackoverflow.com/questions/26519344/sort-search-looking-for-a-number-that-is-not-in-the-slice
	i := sort.Search(len(lb.hashes), func(i int) bool { return lb.hashes[i] >= tempHash })
	if i >= len(lb.hashes) {
		lb.mmeRPCObjectMap[lb.hashes[0]].Call("MME.RecvUERequest", ra, rr)
	} else {
		lb.mmeRPCObjectMap[lb.hashes[i]].Call("MME.RecvUERequest", ra, rr)
	}
	return nil
}

func (lb *loadBalancer) RecvLeave(args *rpcs.LeaveArgs, reply *rpcs.LeaveReply) error {
	// TODO: Implement this!
	// Get the state from leaving MME and remove it from lb.serverNames
	// And its hash from lb.hashes
	lb.physicalNodes--
	lb.virtualNodes = lb.virtualNodes - (lb.ringWeight - 1)
	var sa *rpcs.SendStateArgs = new(rpcs.SendStateArgs)
	var sr *rpcs.SendStateReply = new(rpcs.SendStateReply)
	tempHash := lb.hashObject.Hash(args.HostPort)
	lb.mmeRPCObjectMap[tempHash].Call("MME.RecvSendState", sa, sr)
	lb.hashes = removeUint64(lb.hashes, tempHash)
	lb.serverNames = removeString(lb.serverNames, args.HostPort)
	lb.mmeRPCObjectMap[tempHash].Close()
	delete(lb.mmeRPCObjectMap, tempHash)

	// If virtual nodes are associated with leaving args.HostPort
	// Remove them as well
	if lb.ringWeight > 1 {
		for i := 1; i < lb.ringWeight; i++ {
			virtualHash := lb.hashObject.VirtualNodeHash(args.HostPort, i)
			lb.hashes = removeUint64(lb.hashes, virtualHash)
			delete(lb.mmeRPCObjectMap, virtualHash)
		}
	}
	// Sort the hash ring back to order
	sort.Slice(lb.hashes, func(i, j int) bool { return lb.hashes[i] < lb.hashes[j] })

	// Send the state of leaving MME to all other MMEs
	lb.reallocateKeys(sr.State)

	return nil
}

// RecvLBStats is called by the tests to fetch LB state information
// To pass the tests, please follow the guidelines below carefully.
//
// <reply> (type *rpcs.LBStatsReply) fields must be set as follows:
// RingNodes:			Total number of nodes in the hash ring (physical + virtual)
// PhysicalNodes:		Total number of physical nodes ONLY in the ring
// Hashes:				Sorted List of all the nodes'(physical + virtual) hashes
//						e.g. [5655845225, 789123654, 984545574]
// ServerNames:			List of all the physical nodes' hostPort string as they appear in
// 						the hash ring. e.g. [":5002", ":5001", ":5008"]
func (lb *loadBalancer) RecvLBStats(args *rpcs.LBStatsArgs, reply *rpcs.LBStatsReply) error {
	// TODO: Implement this!
	reply.RingNodes = lb.physicalNodes + lb.virtualNodes
	reply.PhysicalNodes = lb.physicalNodes
	reply.Hashes = lb.hashes
	reply.ServerNames = lb.serverNames
	return nil
}

// TODO: add additional methods/functions below!

func (lb *loadBalancer) RecvJoin(args *rpcs.JoinArgs, reply *rpcs.JoinReply) error {
	lb.physicalNodes++
	lb.virtualNodes = lb.virtualNodes + (lb.ringWeight - 1)
	lb.serverNames = append(lb.serverNames, args.MMEport)
	// We probably don't need to do this
	// sort.Strings(lb.serverNames)
	lb.hashes = append(lb.hashes, lb.hashObject.Hash(args.MMEport))
	tempClient, err := rpc.DialHTTP("tcp", "localhost"+args.MMEport)
	if err != nil {
		return err
	}
	lb.mmeRPCObjectMap[lb.hashObject.Hash(args.MMEport)] = tempClient

	// If virtual nodes exist, assign them the same *rpc.Client
	if lb.ringWeight > 1 {
		for i := 1; i < lb.ringWeight; i++ {
			lb.hashes = append(lb.hashes, lb.hashObject.VirtualNodeHash(args.MMEport, i))
			lb.mmeRPCObjectMap[lb.hashObject.VirtualNodeHash(args.MMEport, i)] = tempClient
		}
	}
	sort.Slice(lb.hashes, func(i, j int) bool { return lb.hashes[i] < lb.hashes[j] })

	// Get state from every other MME and assign them the new states
	for _, conn := range lb.mmeRPCObjectMap {
		var sa *rpcs.SendStateArgs = new(rpcs.SendStateArgs)
		var sr *rpcs.SendStateReply = new(rpcs.SendStateReply)
		conn.Call("MME.RecvSendState", sa, sr)
		lb.reallocateKeys(sr.State)
	}

	return nil
}

// Remove function since slices do not have builtin
func removeUint64(l []uint64, item uint64) []uint64 {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}

// Remove function since slices do not have builtin
func removeString(l []string, item string) []string {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}

func (lb *loadBalancer) reallocateKeys(state map[uint64]rpcs.MMEState) {
	// No need to hash UserID again
	// Just check which UserID maps to which MME
	for k, v := range state {
		var ssa *rpcs.SetStateArgs = new(rpcs.SetStateArgs)
		var ssr *rpcs.SetStateReply = new(rpcs.SetStateReply)
		ssa.UserID = k
		ssa.State = v
		i := sort.Search(len(lb.hashes), func(i int) bool { return lb.hashes[i] >= k })
		if i >= len(lb.hashes) {
			lb.mmeRPCObjectMap[lb.hashes[0]].Call("MME.RecvSetState", ssa, ssr)
		} else {
			lb.mmeRPCObjectMap[lb.hashes[i]].Call("MME.RecvSetState", ssa, ssr)
		}
	}
}
