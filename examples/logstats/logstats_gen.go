package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"time"
)

// generate appends lines "key value" to the file at path, rate a second,
// until ctx is done: requests to paths, and the bytes of their replies,
// some paths far more asked for than others.
func generate(ctx context.Context, path string, rate int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	paths := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 1.2, 1, 999)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	var buf []byte
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		buf = buf[:0]
		for range max(rate/100, 1) {
			buf = fmt.Appendf(buf, "/api/item/%d %d\n", paths.Uint64(), 200+rand.IntN(4000))
		}
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
}
