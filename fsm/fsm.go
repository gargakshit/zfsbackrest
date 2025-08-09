package fsm

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type State[T comparable] struct {
	Name     T
	Terminal bool
}

func (s State[T]) String() string {
	return fmt.Sprintf("FSM state {%v, terminal: %v}", s.Name, s.Terminal)
}

type Transition[StateT comparable] struct {
	From          State[StateT]
	To            State[StateT]
	Run           func() error
	RetryStrategy RetryStrategy
}

func (t Transition[StateT]) String() string {
	return fmt.Sprintf("FSM transition {%v -> %v}", t.From, t.To)
}

type FSM[StateT comparable, ActionT comparable] struct {
	name                 string
	defaultRetryStrategy RetryStrategy
	transitions          map[ActionT]Transition[StateT]
	current              State[StateT]
	lock                 sync.RWMutex
}

func NewFSM[StateT comparable, ActionT comparable](
	name string,
	initialState State[StateT],
	transitions map[ActionT]Transition[StateT],
	defaultRetryStrategy RetryStrategy,
) *FSM[StateT, ActionT] {
	slog.Debug("Creating FSM", "initialState", initialState, "transitions", transitions)

	return &FSM[StateT, ActionT]{
		name:                 name,
		transitions:          transitions,
		current:              initialState,
		defaultRetryStrategy: defaultRetryStrategy,
	}
}

func (f *FSM[StateT, ActionT]) Run(action ActionT) error {
	slog.Debug("Acquiring FSM lock", "name", f.name)
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.current.Terminal {
		slog.Error("FSM is in a terminal state, cannot run action", "name", f.name, "action", action)
		return fmt.Errorf("FSM is in a terminal state, cannot run action %v", action)
	}

	slog.Debug("Running FSM", "name", f.name, "action", action)

	transition, ok := f.transitions[action]
	if !ok {
		slog.Error("Transition not found", "name", f.name, "action", action)
		return fmt.Errorf("transition not found for action %v", action)
	}

	retryStrategy := transition.RetryStrategy
	if retryStrategy == nil {
		slog.Debug("No retry strategy provided, using default", "name", f.name, "action", action)
		retryStrategy = f.defaultRetryStrategy
	}

	retryRunner := retryStrategy.New()

	// Validate that the transition is valid
	if transition.From != f.current {
		slog.Error("Invalid transition", "name", f.name, "action", action, "from", f.current, "to", transition.From)
		return fmt.Errorf("invalid transition from %v to %v", f.current, transition.From)
	}

	for {
		err := transition.Run()
		if err == nil {
			slog.Debug("Transition completed successfully", "name", f.name, "action", action, "from", f.current, "to", transition.To)
			f.current = transition.To
			return nil
		}

		slog.Debug("FSM transition failed, checking if we can retry", "name", f.name, "action", action, "error", err)

		wait, err := retryRunner.RetryAfter(err)
		if err != nil {
			slog.Error("Error retrying", "name", f.name, "action", action, "error", err)
			return err
		}

		slog.Debug("Sleeping before retrying", "name", f.name, "action", action, "wait", wait)
		time.Sleep(wait)
	}
}

func (f *FSM[StateT, ActionT]) CurrentState() State[StateT] {
	slog.Debug("Getting current state", "name", f.name)
	f.lock.RLock()
	defer f.lock.RUnlock()

	return f.current
}

func (f *FSM[StateT, ActionT]) String() string {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return fmt.Sprintf("FSM {%v, current: %v}", f.name, f.current)
}
