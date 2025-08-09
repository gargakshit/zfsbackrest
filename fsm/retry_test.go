package fsm

import (
	"errors"
	"testing"
	"time"
)

func TestIsAndNewUnrecoverableError(t *testing.T) {
	base := errors.New("boom")
	if IsUnrecoverableError(base) {
		t.Fatalf("plain error should not be unrecoverable")
	}

	wrapped := NewUnrecoverableError(base)
	if !IsUnrecoverableError(wrapped) {
		t.Fatalf("wrapped should be unrecoverable")
	}
	if !errors.Is(wrapped, base) {
		t.Fatalf("wrapped should contain base error via errors.Is")
	}
	if !errors.Is(wrapped, UnrecoverableError) {
		t.Fatalf("wrapped should match UnrecoverableError via errors.Is")
	}
}

func TestRetryExponentialBackoff_ZeroMaxRetries(t *testing.T) {
	r := NewRetryExponentialBackoff(RetryExponentialBackoffConfig{MaxRetries: 0, WaitIncrements: time.Millisecond, MaxWait: time.Second})
	d, err := r.RetryAfter(nil)
	if !errors.Is(err, RetryAttemptsExhausted) {
		t.Fatalf("expected RetryAttemptsExhausted, got %v", err)
	}
	if d != 0 {
		t.Fatalf("expected 0 duration on exhaustion, got %v", d)
	}
}

func TestRetryExponentialBackoff_Sequence_NoCap(t *testing.T) {
	cfg := RetryExponentialBackoffConfig{MaxRetries: 5, WaitIncrements: 100 * time.Millisecond, MaxWait: time.Second}
	r := NewRetryExponentialBackoff(cfg)

	var waits []time.Duration
	for {
		d, err := r.RetryAfter(nil)
		if err != nil {
			if !errors.Is(err, RetryAttemptsExhausted) {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
		waits = append(waits, d)
	}

	want := []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond, 400 * time.Millisecond}
	if len(waits) != len(want) {
		t.Fatalf("waits length mismatch: got %d want %d", len(waits), len(want))
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("wait %d mismatch: got %v want %v", i, waits[i], want[i])
		}
	}
}

func TestRetryExponentialBackoff_Sequence_WithCap(t *testing.T) {
	cfg := RetryExponentialBackoffConfig{MaxRetries: 6, WaitIncrements: 100 * time.Millisecond, MaxWait: 250 * time.Millisecond}
	r := NewRetryExponentialBackoff(cfg)

	var waits []time.Duration
	for {
		d, err := r.RetryAfter(nil)
		if err != nil {
			if !errors.Is(err, RetryAttemptsExhausted) {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
		waits = append(waits, d)
	}

	want := []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond}
	if len(waits) != len(want) {
		t.Fatalf("waits length mismatch: got %d want %d", len(waits), len(want))
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("wait %d mismatch: got %v want %v", i, waits[i], want[i])
		}
	}
}

func TestRetryExponentialBackoff_UnrecoverableDoesNotAdvance(t *testing.T) {
	cfg := RetryExponentialBackoffConfig{MaxRetries: 3, WaitIncrements: 100 * time.Millisecond, MaxWait: time.Second}
	r := NewRetryExponentialBackoff(cfg)

	// First call with unrecoverable error should not advance retry counter
	d, err := r.RetryAfter(NewUnrecoverableError(errors.New("boom")))
	if d != 0 {
		t.Fatalf("expected 0 duration for unrecoverable, got %v", d)
	}
	if !IsUnrecoverableError(err) {
		t.Fatalf("expected unrecoverable error, got %v", err)
	}

	// Next call should still be the first retry (wait 0)
	d2, err2 := r.RetryAfter(nil)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if d2 != 0 {
		t.Fatalf("expected 0 duration after unrecoverable (no advance), got %v", d2)
	}
}

func TestRetryExponentialBackoff_ZeroIncrement(t *testing.T) {
	cfg := RetryExponentialBackoffConfig{MaxRetries: 3, WaitIncrements: 0, MaxWait: time.Second}
	r := NewRetryExponentialBackoff(cfg)

	var waits []time.Duration
	for {
		d, err := r.RetryAfter(nil)
		if err != nil {
			if !errors.Is(err, RetryAttemptsExhausted) {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
		waits = append(waits, d)
	}

	want := []time.Duration{0, 0, 0}
	if len(waits) != len(want) {
		t.Fatalf("waits length mismatch: got %d want %d", len(waits), len(want))
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("wait %d mismatch: got %v want %v", i, waits[i], want[i])
		}
	}
}
