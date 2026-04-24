package main

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		body := http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB limit
		var req struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(body).Decode(&req)

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
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	_ = srv.ListenAndServe()
}
