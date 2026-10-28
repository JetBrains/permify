package ristretto

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRistretto(t *testing.T) {
	// Create a new Ristretto cache with default options
	cache, err := New()
	assert.Nil(t, err)

	// Set a value in the cache
	cache.Set("foo", "bar", 1)

	// Wait for the cache to be ready
	cache.Wait()

	// Retrieve the value from the cache
	value, found := cache.Get("foo")
	assert.True(t, found)
	assert.Equal(t, "bar", value.(string))

	// Close the cache
	cache.Close()
}

func TestRistretto_Get(t *testing.T) {
	// Initialize a new Ristretto cache
	cache, err := New(MaxCost("1KB"))
	assert.NoError(t, err)

	// Set a key and value in the cache
	key := "test-key"
	value := "test-value"
	cache.Set(key, value, int64(len(value)))

	// Wait for the cache to be ready
	cache.Wait()

	// Get the value from the cache using the key
	val, found := cache.Get(key)

	// Check that the value was found and is the same as the original value
	assert.True(t, found)
	assert.Equal(t, value, val)

	// Get a non-existent key from the cache
	_, found = cache.Get("non-existent-key")

	// Check that the key was not found
	assert.False(t, found)

	// Close the cache
	cache.Close()
}

func TestRistretto_Set(t *testing.T) {
	// Initialize a new Ristretto cache
	cache, err := New(MaxCost("1KB"))
	assert.NoError(t, err)

	// Set a key and value in the cache
	key := "test-key"
	value := "test-value"
	success := cache.Set(key, value, int64(len(value)))

	// Check that the operation was successful
	assert.True(t, success)

	// Wait for the cache to be ready
	cache.Wait()

	// Get the value from the cache using the key
	val, found := cache.Get(key)

	// Check that the value was found and is the same as the original value
	assert.True(t, found)
	assert.Equal(t, value, val)

	// Close the cache
	cache.Close()
}

func TestRistretto_CloseStopsBackgroundGoroutines(t *testing.T) {
	countRistrettoGoroutines := func() int {
		buf := make([]byte, 1<<20)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				stacks := string(buf[:n])
				return strings.Count(stacks, "ristretto.(*Cache[") +
					strings.Count(stacks, "ristretto.(*defaultPolicy[")
			}
			buf = make([]byte, len(buf)*2)
		}
	}

	before := countRistrettoGoroutines()

	cache, err := New()
	require.NoError(t, err)

	require.Greater(t, countRistrettoGoroutines(), before, "ristretto should spawn background goroutines")

	cache.Close()

	require.Eventually(t, func() bool {
		return countRistrettoGoroutines() == before
	}, time.Second, 5*time.Millisecond, "ristretto background goroutines leaked after Close()")
}
