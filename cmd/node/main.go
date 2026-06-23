package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jules/distributed-search-go/internal/cluster"
	"github.com/jules/distributed-search-go/internal/index"
)

var (
	role        = flag.String("role", "shard", "Role of the node: coordinator or shard")
	port        = flag.Int("port", 8080, "Port to listen on")
	nodeID      = flag.String("id", "shard-1", "Unique ID for the node")
	replicaOf   = flag.String("replica-of", "", "Address of the primary shard if this is a replica")
	shardAddrs  = flag.String("shards", "", "Comma-separated list of shard configurations: primary_addr[;replica_addr]")
	replicaAddr = flag.String("replica-addr", "", "Address of the replica shard (primary shard only)")
)

type IndexRequest struct {
	ID   uint32 `json:"id"`
	Text string `json:"text"`
}

type SearchResponse struct {
	ShardID string   `json:"shard_id"`
	DocIDs  []uint32 `json:"doc_ids"`
}

type SearchJob struct {
	Query string
	Res   chan []uint32
}

var (
	idx      = index.NewInvertedIndex()
	hashRing = cluster.NewHashRing(100)
	client   = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1000,
		},
		Timeout: 2 * time.Second,
	}

	// Coordinator state
	nodeRoundRobin = make(map[string]*uint32)
	nodeReplicas   = make(map[string]string) // primary -> replica

	// Shard state
	searchJobs = make(chan SearchJob, 10000)
)

func main() {
	flag.Parse()

	if *role == "coordinator" {
		runCoordinator()
	} else {
		runShard()
	}
}

func runShard() {
	// Start worker pool
	numWorkers := runtime.NumCPU() * 2
	for i := 0; i < numWorkers; i++ {
		go shardWorker()
	}

	http.HandleFunc("/index", handleIndex)
	http.HandleFunc("/search", handleShardSearch)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	log.Printf("Shard %s starting on port %d with %d workers", *nodeID, *port, numWorkers)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func shardWorker() {
	for job := range searchJobs {
		docIDs := idx.Search(job.Query)
		job.Res <- docIDs
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req IndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	idx.AddDocument(req.ID, req.Text)
	log.Printf("Indexed document %d on shard %s", req.ID, *nodeID)

	if *replicaAddr != "" && *replicaOf == "" {
		go func() {
			body, _ := json.Marshal(req)
			resp, err := client.Post(fmt.Sprintf("http://%s/index", *replicaAddr), "application/json", bytes.NewBuffer(body))
			if err != nil {
				log.Printf("Failed to replicate to %s: %v", *replicaAddr, err)
				return
			}
			defer resp.Body.Close()
		}()
	}

	w.WriteHeader(http.StatusOK)
}

func handleShardSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	resChan := make(chan []uint32)
	searchJobs <- SearchJob{Query: query, Res: resChan}
	docIDs := <-resChan

	json.NewEncoder(w).Encode(SearchResponse{
		ShardID: *nodeID,
		DocIDs:  docIDs,
	})
}

func runCoordinator() {
	if *shardAddrs != "" {
		// New format: shard1-p:8080[;shard1-r:8080],shard2-p:8080[;shard2-r:8080]
		configs := strings.Split(*shardAddrs, ",")
		for _, config := range configs {
			parts := strings.Split(config, ";")
			primary := parts[0]
			hashRing.AddNode(primary)
			var counter uint32
			nodeRoundRobin[primary] = &counter
			if len(parts) > 1 {
				nodeReplicas[primary] = parts[1]
			}
		}
	}

	go healthCheckLoop()

	http.HandleFunc("/search", handleCoordinatorSearch)
	http.HandleFunc("/index", handleCoordinatorIndex)
	log.Printf("Coordinator starting on port %d", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		for primary, replica := range nodeReplicas {
			checkNode(primary)
			if replica != "" {
				checkNode(replica)
			}
		}
	}
}

func checkNode(addr string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/health", addr))
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("Node %s is down", addr)
		if hashRing.IsNodeActive(addr) {
			hashRing.RemoveNode(addr)
		}
	} else {
		if !hashRing.IsNodeActive(addr) {
			hashRing.AddNode(addr)
		}
		resp.Body.Close()
	}
}

func handleCoordinatorIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req IndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Term-based routing: Hash the document text terms? No, blueprint says:
	// "To find where a document (or search term) belongs: Hash the Document ID" (Step 3.3)
	// Actually, Step 4.2 says: "The Coordinator parses this into two terms. It consults the Consistent Hash ring and finds that Term A is on Shard 2, and Term B is on Shard 4."
	// This implies Terms are hashed.

	// If I follow Term-based sharding:
	tokens := strings.Fields(strings.ToLower(req.Text))
	uniqueTerms := make(map[string]bool)
	for _, t := range tokens {
		t = strings.Trim(t, ".,!?;:()\"'")
		if t != "" {
			uniqueTerms[t] = true
		}
	}

	var wg sync.WaitGroup
	for term := range uniqueTerms {
		shardAddr, ok := hashRing.GetNode(term)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(addr, t string) {
			defer wg.Done()
			body, _ := json.Marshal(req)
			resp, err := client.Post(fmt.Sprintf("http://%s/index", addr), "application/json", bytes.NewBuffer(body))
			if err == nil {
				resp.Body.Close()
			}
		}(shardAddr, term)
	}
	wg.Wait()

	w.WriteHeader(http.StatusOK)
}

func handleCoordinatorSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	terms := strings.Fields(strings.ToLower(query))
	if len(terms) == 0 {
		json.NewEncoder(w).Encode([]uint32{})
		return
	}

	var wg sync.WaitGroup
	resultChan := make(chan []uint32, len(terms))

	for _, term := range terms {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			shardAddr, ok := hashRing.GetNode(t)
			if !ok {
				return
			}

			// Load balance between primary and replica
			target := shardAddr
			replica := nodeReplicas[shardAddr]
			if replica != "" {
				if counter, ok := nodeRoundRobin[shardAddr]; ok {
					if atomic.AddUint32(counter, 1)%2 == 1 {
						target = replica
					}
				}
			}

			res, err := client.Get(fmt.Sprintf("http://%s/search?q=%s", target, url.QueryEscape(t)))
			if err != nil {
				// Retry with the other one if available
				if target == shardAddr && replica != "" {
					target = replica
				} else {
					target = shardAddr
				}
				res, err = client.Get(fmt.Sprintf("http://%s/search?q=%s", target, url.QueryEscape(t)))
				if err != nil {
					return
				}
			}
			defer res.Body.Close()

			var searchResp SearchResponse
			if err := json.NewDecoder(res.Body).Decode(&searchResp); err == nil {
				resultChan <- searchResp.DocIDs
			}
		}(term)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var sets [][]uint32
	for set := range resultChan {
		sets = append(sets, set)
	}

	// AND logic across terms
	finalDocIDs := index.Intersect(sets...)
	if finalDocIDs == nil {
		finalDocIDs = []uint32{}
	}

	json.NewEncoder(w).Encode(finalDocIDs)
}
