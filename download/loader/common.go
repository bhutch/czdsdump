package loader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type Chunk struct {
	Index  int
	Buffer *bytes.Buffer
	Err    error
}

func calcChunkSize(totalSize, workers int) int {
	v := float64(totalSize) / float64(workers)
	return int(math.Ceil(v))
}

func downloadChunk(ctx context.Context, index, size, workers int, accessToken, zoneURL string, chunks chan *Chunk) {
	client := http.Client{Timeout: time.Minute * 30}
	chunk := &Chunk{Index: index, Buffer: &bytes.Buffer{}}
	// Always send the chunk so download() never hangs waiting
	defer func() { chunks <- chunk }()
	startBytes := index * size
	endBytes := startBytes + size - 1
	reqRange := fmt.Sprintf("bytes=%d-%d", startBytes, endBytes)
	isFinalChunk := workers-1 == index
	if isFinalChunk {
		reqRange = fmt.Sprintf("bytes=%d-", startBytes)
	}
	req, err := http.NewRequestWithContext(ctx, "GET", zoneURL, nil)
	if err != nil {
		chunk.Err = fmt.Errorf("chunk %d: create request: %w", index, err)
		return
	}
	req.Header.Add("Range", reqRange)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	var resp *http.Response
	do := func() error {
		resp, err = client.Do(req)
		return err
	}
	backOffCtx := backoff.WithContext(backoff.NewConstantBackOff(time.Minute*1), ctx)
	retry := backoff.WithMaxRetries(backOffCtx, 2)
	if err := backoff.Retry(do, retry); err != nil {
		chunk.Err = fmt.Errorf("chunk %d: download failed after retries: %w", index, err)
		return
	}
	defer resp.Body.Close()
	n, copyErr := io.Copy(chunk.Buffer, resp.Body)
	if copyErr != nil {
		chunk.Err = fmt.Errorf("chunk %d: read failed after %d bytes: %w", index, n, copyErr)
		return
	}
	// Validate size for non-final chunks
	if !isFinalChunk && n != int64(size) {
		chunk.Err = fmt.Errorf("chunk %d: expected %d bytes, got %d (truncated)", index, size, n)
	}
}

func download(ctx context.Context, accessToken, zoneURL string, numWorkers int, fileChunks chan *Chunk) (io.Reader, error) {
	client := http.Client{Timeout: time.Second * 120}
	req, err := http.NewRequestWithContext(ctx, "HEAD", zoneURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		return nil, fmt.Errorf("could not get Content-Length header")
	}
	fileSize, err := strconv.Atoi(contentLength)
	if err != nil {
		return nil, err
	}
	chunkSize := calcChunkSize(fileSize, numWorkers)
	for i := range numWorkers {
		go downloadChunk(
			ctx, i, chunkSize, numWorkers, accessToken, zoneURL, fileChunks)
	}
	chunkCount := 0
	chunkBuffers := make([]io.Reader, numWorkers)
	for chunk := range fileChunks {
		if chunk.Err != nil {
			return nil, fmt.Errorf("download %s: %w", zoneURL, chunk.Err)
		}
		chunkBuffers[chunk.Index] = chunk.Buffer
		chunkCount++
		if chunkCount == numWorkers {
			break
		}
	}
	return io.MultiReader(chunkBuffers[:]...), nil
}
