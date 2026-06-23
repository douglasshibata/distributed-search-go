package index

import (
	"strings"
	"sync"
)

// Posting stores information about a term's occurrence in a document.
type Posting struct {
	DocID    uint32
	Position []int // Word positions for phrase queries
}

// InvertedIndex maps terms to their postings and stores document metadata.
type InvertedIndex struct {
	mu       sync.RWMutex
	Postings map[string][]Posting
	Docs     map[uint32]string
}

// NewInvertedIndex initializes a new InvertedIndex.
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		Postings: make(map[string][]Posting),
		Docs:     make(map[uint32]string),
	}
}

// AddDocument tokenizes the text and updates the inverted index.
func (idx *InvertedIndex) AddDocument(id uint32, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.Docs[id] = text
	// Simple tokenization: lowercase and split by whitespace.
	tokens := strings.Fields(strings.ToLower(text))

	// Group positions by term for the current document.
	termPositions := make(map[string][]int)
	for pos, token := range tokens {
		token = strings.Trim(token, ".,!?;:()\"'")
		if token == "" {
			continue
		}
		termPositions[token] = append(termPositions[token], pos)
	}

	for term, positions := range termPositions {
		// Check if this DocID is already in the postings for this term (idempotency)
		exists := false
		for _, p := range idx.Postings[term] {
			if p.DocID == id {
				exists = true
				break
			}
		}
		if !exists {
			idx.Postings[term] = append(idx.Postings[term], Posting{
				DocID:    id,
				Position: positions,
			})
		}
	}
}

// Search returns a list of document IDs that contain the given term.
func (idx *InvertedIndex) Search(term string) []uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	term = strings.ToLower(strings.TrimSpace(term))
	postings, ok := idx.Postings[term]
	if !ok {
		return []uint32{}
	}

	docIDs := make([]uint32, len(postings))
	for i, p := range postings {
		docIDs[i] = p.DocID
	}
	return docIDs
}

// Intersect returns the intersection of multiple DocID slices (for AND queries).
func Intersect(sets ...[]uint32) []uint32 {
	if len(sets) == 0 {
		return []uint32{}
	}
	if len(sets) == 1 {
		return sets[0]
	}

	// Use a map to count occurrences, ensuring we only count each ID once per set.
	counts := make(map[uint32]int)
	for _, set := range sets {
		setUnique := make(map[uint32]struct{})
		for _, id := range set {
			setUnique[id] = struct{}{}
		}
		for id := range setUnique {
			counts[id]++
		}
	}

	var result []uint32
	for id, count := range counts {
		if count == len(sets) {
			result = append(result, id)
		}
	}
	return result
}
