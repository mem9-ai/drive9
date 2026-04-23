package main

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		vec := make([]float32, 1024)
		for i := range vec {
			vec[i] = rand.Float32()
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"embedding": vec, "index": 0, "object": "embedding"},
			},
			"model":  "stub",
			"object": "list",
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "11435"
	}
	_ = http.ListenAndServe(":"+port, nil)
}
