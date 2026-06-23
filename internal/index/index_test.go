package index

import (
	"reflect"
	"sort"
	"testing"
)

func TestInvertedIndex(t *testing.T) {
	idx := NewInvertedIndex()

	idx.AddDocument(1, "Golang is great for distributed systems")
	idx.AddDocument(2, "Docker helps with environment isolation")
	idx.AddDocument(3, "Distributed search with Go and Docker")

	tests := []struct {
		term     string
		expected []uint32
	}{
		{"golang", []uint32{1}},
		{"go", []uint32{3}},
		{"docker", []uint32{2, 3}},
		{"distributed", []uint32{1, 3}},
		{"search", []uint32{3}},
		{"missing", []uint32{}},
	}

	for _, tt := range tests {
		got := idx.Search(tt.term)
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		sort.Slice(tt.expected, func(i, j int) bool { return tt.expected[i] < tt.expected[j] })

		if !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("Search(%q) = %v, want %v", tt.term, got, tt.expected)
		}
	}
}

func TestIntersect(t *testing.T) {
	s1 := []uint32{1, 2, 3}
	s2 := []uint32{2, 3, 4}
	s3 := []uint32{3, 4, 5}

	got := Intersect(s1, s2, s3)
	expected := []uint32{3}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Intersect = %v, want %v", got, expected)
	}

	gotEmpty := Intersect([]uint32{1}, []uint32{2})
	if len(gotEmpty) != 0 {
		t.Errorf("Intersect of non-overlapping should be empty, got %v", gotEmpty)
	}
}
