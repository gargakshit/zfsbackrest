package config

import "time"

type Repository struct {
	Expiry Expiry
}

type Expiry struct {
	Full time.Duration
	Diff time.Duration
	Incr time.Duration
}
