package ib

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// UpdateStatus .
type UpdateStatus int

// Status enum
const (
	UpdateFalse UpdateStatus = 1 << iota
	UpdateTrue
	UpdateFinish
)

// Manager provides a high-level abstraction over selected IB API use cases.
// It defines a contract for executing IB API operations with explicit
// isolation, error handling and concurrency guarantees.
//
// To use a Manager, invoke its NewXXManager function. This will immediately
// return an XXManager value that will subsequentially asynchronously connect to
// the Engine and send data. The only errors returned from a NewXXManager
// function relate to invalid parameters, as Engine state is unknown until the
// asynchronous connection is attempted at an unspecified later time.
//
// A Manager provides idempotent accessors to obtain data or the state of the
// construction-specified operation. Accessors guarantee to never return data
// from a partially-consumed IB API Reply. They may return zero or nil if data
// isn't yet available. Accessors remain usable even after a Manager is closed.
//
// Clients must invoke the Refresh() method to obtain the channel that will be
// signalled whenever the Manager has new data available from one or more
// accessors. The client must notify the manager that the data has been read using
// the object received from the channel.
// The Manager will close the refresh channel when no further updates
// will be provided. This may occur for three reasons: (i) a client has invoked
// Close(); (ii) the Manager encountered an error (which is available via
// FatalError()) or (iii) the Manager is aware no new data will be sent by this
// Manager (typically as the IB API has sent an "end of data" Reply). In either
// of the latter two situations the Manager will automatically unsubscribe from
// IB API subscriptions and Engine subscriptions (this is a performance
// optimisation only; users should still use "defer manager.Close()").
//
// A Manager will block until a client can consume from the refresh channel.
// If the client needs to perform long-running operations, it should consider
// dedicating a goroutine to consuming from the channel and maintaining a count
// of update signals. This would permit the client to discover it missed an
// update without blocking the Manager (or its upstream Engine). SinkManager is
// another option for clients only interested in the final result of a Manager.
//
// Managers will register for Engine state changes. If the Engine exits, the
// Manager will close. If Engine.FatalError() returns an error, it will be made
// available via manager.FatalError() (unless an earlier error was recorded).
// This means clients need not track Manager errors or states themselves.
//
// Every Manager defines a Close() method which blocks until the Manager has
// released its resources. The Close() method will not return any new error or
// change the state of FatalError(), even if it encounters errors (eg Engine
// send failure) while closing its resources. After a Manager is closed, its
// accessors remain available. Repeatedly calling Close() is safe and will not
// repeatedly release any already-released resources. Therefore it is safe and
// highly recommended to use "defer manager.Close()".
type Manager interface {
	FatalError() error
	Refresh() <-chan Acknowledger
	Close()
}

// Acknowledger defines a contract to acknowledge Manager that the new data from
// one or more accessors is read. It prevents a possible data race when the client
// receives a notification from the manager about the data update, but after the
// notification, the manager does not wait for the client to read the data, but
// updates with an even newer version. To avoid this data race, the client must
// call Acknowledge method to notify the manager that the data has been read.
type Acknowledger interface {
	Acknowledge()
}

// Ack implements Acknowledger interface and allows Manager to wait until the data
// is read by the client.
type Ack struct {
	ackOnce sync.Once
	ack     chan struct{}
}

// NewAck creates new instance of Ack which implements Acknowledger interface.
func NewAck() *Ack {
	return &Ack{
		ack: make(chan struct{}),
	}
}

// WaitForAcknowledgment is called by the Manager to wait until the data is read by
// the client.
func (a *Ack) WaitForAcknowledgment() {
	if a == nil {
		return
	}
	if ack := a.ack; ack != nil {
		<-ack
	}
}

// Acknowledge is called by the client to notify Manager that the data has been read.
// It implements Acknowledger interface.
func (a *Ack) Acknowledge() {
	if a == nil {
		return
	}
	if ack := a.ack; ack != nil {
		a.ackOnce.Do(func() {
			close(ack)
		})
	}
}

// AbstractManager implements most of the Manager interface contract.
type AbstractManager struct {
	rwm    sync.RWMutex
	term   chan struct{}
	exit   chan bool
	update chan Acknowledger
	engs   chan EngineState
	eng    *Engine
	err    error
	rc     chan Reply
}

// NewAbstractManager creates the manager that implement most of the Manager interface contract.
func NewAbstractManager(e *Engine) (*AbstractManager, error) {
	if e == nil {
		return nil, errors.New("engine required")
	}
	am := &AbstractManager{
		rwm:    sync.RWMutex{},
		term:   make(chan struct{}),
		exit:   make(chan bool),
		update: make(chan Acknowledger),
		engs:   make(chan EngineState),
		eng:    e,
		rc:     make(chan Reply),
	}
	return am, nil
}

// Engine returns Engine
func (a *AbstractManager) Engine() *Engine {
	return a.eng
}

// ReplyCh returns ReplyCh
func (a *AbstractManager) ReplyCh() chan Reply {
	return a.rc
}

// StartMainLoop is method that needs to be run in a separate goroutine to handle events.
// It's exported to allow implement specific manager in external packages that import ib package.
func (a *AbstractManager) StartMainLoop(preLoop func() error, receive func(r Reply) (UpdateStatus, error), preDestroy func()) {
	preLoopError := make(chan error, 1) // 1 - to guarantee that preLoop won't hang on writing to the channel, if we exit the for/select loop earlier for some reason
	preLoopFinished := make(chan struct{})

	defer func() {
		<-preLoopFinished // ensures preLoop goroutine has exited
		preDestroy()
		a.eng.UnsubscribeState(a.engs)
		close(a.update)
		close(a.term)
	}()

	go a.eng.SubscribeState(a.engs)
	go func() {
		preLoopError <- preLoop()
		close(preLoopFinished)
	}()

	for {
		select {
		case <-a.exit:
			return
		case e := <-preLoopError:
			preLoopError = nil // ignore listening on the channel
			if e != nil {
				a.err = e
				return
			}
		case r := <-a.rc:
			if a.consume(r, receive) {
				return
			}
		case <-a.engs:
			if a.err == nil {
				a.err = a.eng.FatalError()
			}
			return
		}
	}
}

// consume handles sending one Reply to the receive function. Returning true
// indicates the main loop should terminate (ie the AbstractManager close).
func (a *AbstractManager) consume(r Reply, receive func(r Reply) (UpdateStatus, error)) (exit bool) {
	updStatus := make(chan UpdateStatus)

	go func() { // new goroutine to guarantee unlock
		a.rwm.Lock()
		defer a.rwm.Unlock()
		status, err := receive(r)
		if err != nil {
			a.err = err
			close(updStatus)
			return
		}
		updStatus <- status
	}()

	status, ok := <-updStatus
	if !ok {
		return true // channel closed due to receive func error result
	}
	switch status {
	case UpdateFalse:
	case UpdateTrue:
		ack := NewAck()
		a.update <- ack
		ack.WaitForAcknowledgment()
	case UpdateFinish:
		ack := NewAck()
		a.update <- ack
		ack.WaitForAcknowledgment()
		return true
	}
	return false
}

// RLock .
func (a *AbstractManager) RLock() { a.rwm.RLock() }

// RUnlock .
func (a *AbstractManager) RUnlock() { a.rwm.RUnlock() }

// FatalError .
func (a *AbstractManager) FatalError() error {
	return a.err
}

// Refresh .
func (a *AbstractManager) Refresh() <-chan Acknowledger {
	return a.update
}

// Close .
func (a *AbstractManager) Close() {
	select {
	case <-a.term:
		return
	case a.exit <- true:
	}
	<-a.term
}

// SinkManager listens to a Manager until it closes the update channel or reaches
// a target update count or timeout. This function is useful for clients who only
// require the final result of a Manager (and have no interest in each update).
// The Manager is guaranteed to be closed before it returns.
func SinkManager(m Manager, timeout time.Duration, updateStop int) (updates int, err error) {
	idleTimer := time.NewTimer(timeout)
	defer idleTimer.Stop()
	for {
		sentClose := false
		idleTimer.Reset(timeout)
		select {
		case <-idleTimer.C:
			m.Close()
			return updates, fmt.Errorf("SinkManager: no new update in %s", timeout)
		case ack, ok := <-m.Refresh():
			if !ok {
				return updates, m.FatalError()
			}
			ack.Acknowledge()
			updates++
			if updates >= updateStop && !sentClose {
				sentClose = true
				go m.Close()
			}
		}
	}
}
