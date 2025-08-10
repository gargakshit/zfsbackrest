package fsm

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type State[T comparable, D any] struct {
	ID       T
	Data     *D
	Terminal bool
}

func (s State[T, D]) String() string {
	return fmt.Sprintf("FSM state {%v, data: %v, terminal: %v}", s.ID, s.Data, s.Terminal)
}

type Transition[StateT comparable, DataT any] struct {
	From          StateT
	To            StateT
	Run           func(ctx context.Context, data *DataT) error
	RetryStrategy RetryStrategy
}

func (t Transition[StateT, DataT]) String() string {
	return fmt.Sprintf("FSM transition {%v -> %v}", t.From, t.To)
}

type FSM[StateT comparable, ActionT comparable, DataT any] struct {
	name                 string
	defaultRetryStrategy RetryStrategy
	transitions          map[ActionT]Transition[StateT, DataT]
	current              State[StateT, DataT]
	lock                 sync.RWMutex
}

func NewFSM[StateT comparable, ActionT comparable, DataT any](
	name string,
	initialState State[StateT, DataT],
	transitions map[ActionT]Transition[StateT, DataT],
	defaultRetryStrategy RetryStrategy,
) *FSM[StateT, ActionT, DataT] {
	slog.Debug("Creating FSM", "initialState", initialState, "transitions", transitions)

	return &FSM[StateT, ActionT, DataT]{
		name:                 name,
		transitions:          transitions,
		current:              initialState,
		defaultRetryStrategy: defaultRetryStrategy,
	}
}

func (f *FSM[StateT, ActionT, DataT]) Run(ctx context.Context, action ActionT) error {
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
	if transition.From != f.current.ID {
		slog.Error("Invalid transition", "name", f.name, "action", action, "from", f.current, "to", transition.From)
		return fmt.Errorf("invalid transition from %v to %v", f.current, transition.From)
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, cancelling FSM transition", "name", f.name, "action", action)
			return ctx.Err()
		default:
		}

		err := transition.Run(ctx, f.current.Data)
		if err == nil {
			slog.Debug("Transition completed successfully", "name", f.name, "action", action, "from", f.current, "to", transition.To)
			f.current = State[StateT, DataT]{
				ID:       transition.To,
				Data:     f.current.Data,
				Terminal: f.current.Terminal,
			}
			return nil
		}

		slog.Debug("FSM transition failed, checking if we can retry", "name", f.name, "action", action, "error", err)

		select {
		case <-ctx.Done():
			slog.Error("Context cancelled, cancelling FSM retry", "name", f.name, "action", action)
			return ctx.Err()
		default:
		}

		wait, err := retryRunner.RetryAfter(err)
		if err != nil {
			slog.Error("Error retrying", "name", f.name, "action", action, "error", err)
			return err
		}

		slog.Debug("Sleeping before retrying", "name", f.name, "action", action, "wait", wait)
		time.Sleep(wait)
	}
}

func (f *FSM[StateT, ActionT, DataT]) RunSequence(ctx context.Context, actions ...ActionT) error {
	slog.Debug("Running FSM sequence", "name", f.name, "actions", actions)

	for _, action := range actions {
		slog.Debug("Running FSM action", "name", f.name, "action", action)
		err := f.Run(ctx, action)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *FSM[StateT, ActionT, DataT]) CurrentState() State[StateT, DataT] {
	slog.Debug("Getting current state", "name", f.name)
	f.lock.RLock()
	defer f.lock.RUnlock()

	return f.current
}

func (f *FSM[StateT, ActionT, DataT]) String() string {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return fmt.Sprintf("FSM {%v, current: %v}", f.name, f.current)
}
