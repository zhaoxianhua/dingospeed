package data

import (
	"dingospeed/pkg/config"

	"github.com/dgraph-io/ristretto/v2"
)

type Cache[K comparable, V any] interface {
	Set(key K, value V)
	Get(key K) (V, bool)
	Delete(key K)
	Wait()
}

type RistrettoCache[K string, V any] struct {
	RCache *ristretto.Cache[K, V]
	cost   int64
}

func (f *RistrettoCache[K, V]) Set(key K, value V) {
	f.RCache.SetWithTTL(key, value, f.cost, config.SysConfig.GetPrefetchBlockTTL())
}

func (f *RistrettoCache[K, V]) Get(key K) (V, bool) {
	return f.RCache.Get(key)
}

func (f *RistrettoCache[K, V]) Delete(key K) {
	f.RCache.Del(key)
}
func (f *RistrettoCache[K, V]) Wait() {
	f.RCache.Wait()
}
