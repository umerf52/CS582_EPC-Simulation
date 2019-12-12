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
	physicalNodeMap                                            map[uint64]*rpc.Client
	mmeStateMap                                                map[uint64]map[uint64]rpcs.MMEState
	phyToVNodeMap                                              map[*rpc.Client][]uint64
	vNodeToServMap                                             map[uint64]string
}

// var LOGF *log.Logger

// New returns a new instance of LoadBalancer, but does not start it
func New(ringWeight int) LoadBalancer {
	// const (
	// 	name = "lblog.txt"
	// 	flag = os.O_RDWR | os.O_CREATE
	// 	perm = os.FileMode(0666)
	// )

	// file, err := os.OpenFile(name, flag, perm)
	// if err != nil {
	// 	//return nil
	// }
	// //defer file.Close()
	// LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// LOGF.Println(ringWeight)
	// TODO: Implement this!
	var lb *loadBalancer
	lb = new(loadBalancer)
	lb.ringWeight = ringWeight
	lb.physicalNodes = 0
	lb.virtualNodes = 0
	lb.hashes = make([]uint64, 0)
	lb.serverNames = make([]string, 0)
	lb.hashObject = new(ConsistentHashing)
	lb.mmeStateMap = make(map[uint64]map[uint64]rpcs.MMEState)
	lb.mmeRPCObjectMap = make(map[uint64]*rpc.Client)
	lb.phyToVNodeMap = make(map[*rpc.Client][]uint64)
	lb.vNodeToServMap = make(map[uint64]string)
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

	// Logic to find
	//index taken from: https://stackoverflow.com/questions/26519344/sort-search-looking-for-a-number-that-is-not-in-the-slice
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
	delete(lb.vNodeToServMap, tempHash)

	// If virtual nodes are associated with leaving args.HostPort
	// Remove them as well
	if lb.ringWeight > 1 {
		for i := 1; i < lb.ringWeight; i++ {
			virtualHash := lb.hashObject.VirtualNodeHash(args.HostPort, i)

			lb.hashes = removeUint64(lb.hashes, virtualHash)
			delete(lb.mmeRPCObjectMap, virtualHash)
			delete(lb.vNodeToServMap, virtualHash)

		}
	}
	// Sort the hash ring back to order
	sort.Slice(lb.hashes, func(i, j int) bool { return lb.hashes[i] < lb.hashes[j] })
	//if lb.virtualNodes == 0 {

	replicas := lb.getReplicas()
	//LOGF.Println(len(replicas))

	//LOGF.Println(replicas)
	//reply.Replicas = replicas
	for conn, repl := range replicas {
		repl = removeDuplicates(repl)
		var ra *rpcs.SetReplicaArgs = new(rpcs.SetReplicaArgs)
		var rr *rpcs.SetReplicaReply = new(rpcs.SetReplicaReply)
		ra.Replicas = repl
		//fmt.Println(repl)
		conn.Call("MME.RecvReplicas", ra, rr)
	}
	//}

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
	lb.hashes = append(lb.hashes, lb.hashObject.Hash(args.MMEport))
	tempClient, err := rpc.DialHTTP("tcp", "localhost"+args.MMEport)
	if err != nil {
		return err
	}
	// tempMmeMap := make(map[uint64]*rpc.Client)
	// tempMmeMap = lb.mmeRPCObjectMap
	// tempPhysicalMap := make(map[uint64]*rpc.Client)
	// tempPhysicalMap = lb.physicalNodeMap

	//lb.physicalNodeMap[lb.hashObject.Hash(args.MMEport)] = tempClient
	lb.mmeRPCObjectMap[lb.hashObject.Hash(args.MMEport)] = tempClient
	lb.vNodeToServMap[lb.hashObject.Hash(args.MMEport)] = args.MMEport

	// for k,v:= range replicaMap{

	// }

	// If virtual nodes exist, assign them the same *rpc.Client
	if lb.ringWeight > 1 {
		for i := 1; i < lb.ringWeight; i++ {
			lb.hashes = append(lb.hashes, lb.hashObject.VirtualNodeHash(args.MMEport, i))
			lb.mmeRPCObjectMap[lb.hashObject.VirtualNodeHash(args.MMEport, i)] = tempClient
			lb.vNodeToServMap[lb.hashObject.VirtualNodeHash(args.MMEport, i)] = args.MMEport
			lb.phyToVNodeMap[tempClient] = append(lb.phyToVNodeMap[tempClient], lb.hashObject.VirtualNodeHash(args.MMEport, i))
		}
	}
	sort.Slice(lb.hashes, func(i, j int) bool { return lb.hashes[i] < lb.hashes[j] })
	lb.sortServerByHash()
	//fmt.Println(lb.hashes)
	//fmt.Println(lb.serverNames)
	//if lb.virtualNodes == 0 {

	replicas := lb.getReplicas()

	//LOGF.Println(len(replicas))

	//LOGF.Println(replicas)
	//reply.Replicas = replicas
	for conn, repl := range replicas {
		repl = removeDuplicates(repl)
		var ra *rpcs.SetReplicaArgs = new(rpcs.SetReplicaArgs)
		var rr *rpcs.SetReplicaReply = new(rpcs.SetReplicaReply)
		ra.Replicas = repl
		//fmt.Println(repl)
		conn.Call("MME.RecvReplicas", ra, rr)
	}
	//}

	// Get state from every other MME and assign them the new states
	for k, conn := range lb.mmeRPCObjectMap {
		var sa *rpcs.SendStateArgs = new(rpcs.SendStateArgs)
		var sr *rpcs.SendStateReply = new(rpcs.SendStateReply)
		conn.Call("MME.RecvSendState", sa, sr)

		lb.mmeStateMap[k] = sr.State

		lb.reallocateKeys(sr.State)
	}

	// for _, state := range lb.mmeStateMap {
	// 	lb.reallocateKeys(state)
	// 	//delete(lb.mmeStateMap, k)
	// }
	// lb.mmeStateMap = make(map[uint64]map[uint64]rpcs.MMEState)
	// for k := range lb.mmeStateMap {
	// 	delete(lb.mmeStateMap, k)
	// }

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

func (lb *loadBalancer) sortServerByHash() {

	tempServerNames := make([]string, 0)
	for _, hash := range lb.hashes {
		for _, server := range lb.serverNames {
			tempHash := lb.hashObject.Hash(server)
			if tempHash == hash {
				tempServerNames = append(tempServerNames, server)
				break
			}
		}
	}
	lb.serverNames = tempServerNames

}

func (lb *loadBalancer) getReplicas() map[*rpc.Client][]string {
	//fmt.Println(lb.vNodeToServMap)
	// fmt.Println("-------------- STARTING PRINT------------ ")
	// for k, v := range lb.vNodeToServMap {
	// 	fmt.Println(k, v)
	// }
	replicaMap := make(map[*rpc.Client][]string)
	passComplete := false
	for i, hash := range lb.hashes {
		conn := lb.mmeRPCObjectMap[hash]

		replicas := make([]string, 0)

		if i != len(lb.hashes)-1 {
			myServer := lb.vNodeToServMap[hash]
			nextServer := lb.vNodeToServMap[lb.hashes[i+1]]
			//conn2 := lb.mmeRPCObjectMap[lb.hashes[i+1]]

			if myServer != nextServer {

				if _, ok := replicaMap[conn]; ok {
					replicaMap[conn] = append(replicaMap[conn], nextServer)
				} else {
					replicas = append(replicas, nextServer)
					replicaMap[conn] = replicas

				}

			} else {
				temp := i
				j := i + 1

				for myServer == nextServer {
					nextServer = lb.vNodeToServMap[lb.hashes[j]]
					if j == temp {
						passComplete = true
						break
					}
					j++
					if j > len(lb.hashes)-1 {
						j = 0
					}

				}

				if passComplete {
					replicaMap[conn] = replicas
					passComplete = false
				}

				if _, ok := replicaMap[conn]; ok {
					replicaMap[conn] = append(replicaMap[conn], nextServer)
				} else {
					replicas = append(replicas, nextServer)
					replicaMap[conn] = replicas

				}

			}

		} else if len(lb.hashes) == 1 {
			replicaMap[conn] = replicas
		} else {
			myServer := lb.vNodeToServMap[hash]
			nextServer := lb.vNodeToServMap[lb.hashes[0]]
			//conn2 := lb.mmeRPCObjectMap[lb.hashes[i+1]]

			if myServer != nextServer {

				if _, ok := replicaMap[conn]; ok {
					replicaMap[conn] = append(replicaMap[conn], nextServer)
				} else {
					replicas = append(replicas, nextServer)
					replicaMap[conn] = replicas

				}

			} else {

				j := 0

				for myServer == nextServer {
					nextServer = lb.vNodeToServMap[lb.hashes[j]]
					j++
					if j > len(lb.hashes)-1 {
						j = 0
						replicaMap[conn] = replicas
						return replicaMap
					}

				}

				if _, ok := replicaMap[conn]; ok {
					replicaMap[conn] = append(replicaMap[conn], nextServer)
				} else {
					replicas = append(replicas, nextServer)
					replicaMap[conn] = replicas

				}

			}

		}
	}

	//i := lb.getPos(hash)
	// conn := lb.mmeRPCObjectMap[hash]
	// for i, v := range lb.hashes {
	// 	//fmt.Println(i)
	// 	replicas := make([]string, 0)
	// 	conn := lb.mmeRPCObjectMap[v]
	// 	replicaMap[conn] = make([]string, 0)
	// 	if i != len(lb.hashes)-1 {

	// 		//server1 := lb.getServerName(lb.hashes[i+1])
	// 		//fmt.Println(lb.serverNames[i+1])
	// 		replicas = append(replicas, lb.serverNames[i+1])
	// 		//fmt.Println(replicas)
	// 		replicaMap[conn] = replicas
	// 	} else if len(lb.hashes) == 1 {
	// 		replicaMap[conn] = replicas
	// 		//return replicas
	// 	} else {
	// 		//LOGF.Println(lb.serverNames[0])
	// 		//fmt.Println(lb.serverNames[0])
	// 		//server1 := lb.getServerName(lb.hashes[0])
	// 		replicas = append(replicas, lb.serverNames[0])
	// 		replicaMap[conn] = replicas
	// 		//fmt.Println(replicas)
	// 	}
	// }

	return replicaMap
	//return replicas

}

func (lb *loadBalancer) getPos(hash uint64) int {

	for i, hash1 := range lb.hashes {
		if hash1 == hash {
			return i
		}
	}
	return -1
}

func (lb *loadBalancer) getServerName(hash uint64) string {
	for _, server := range lb.serverNames {

		if hash == lb.hashObject.Hash(server) {
			return server
		}
	}
	return "-1"
}

func removeDuplicates(replicas []string) []string {
	tempReplicaMap := make(map[string]bool)
	tempReplica := make([]string, 0)
	for _, replica := range replicas {
		tempReplicaMap[replica] = true
	}
	for k := range tempReplicaMap {
		tempReplica = append(tempReplica, k)
	}
	return tempReplica
}

func elementCount(ele string, arr []string) int {
	count := 0
	for _, val := range arr {
		if val == ele {
			count++
		}
	}
	return count
}
