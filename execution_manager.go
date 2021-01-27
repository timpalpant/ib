package ib

import (
	"fmt"
)

// ExecutionManager fetches execution reports from the past 24 hours.
type ExecutionManager struct {
	Manager
	id     int64
	filter ExecutionFilter
	values []ExecutionData
}

// NewExecutionManager .
func NewExecutionManager(e *Engine, filter ExecutionFilter) (*ExecutionManager, error) {
	am, err := NewAbstractManager(e)
	if err != nil {
		return nil, err
	}

	em := &ExecutionManager{Manager: am,
		id:     UnmatchedReplyID,
		filter: filter,
	}

	go em.am().StartMainLoop(em.preLoop, em.receive, em.preDestroy)
	return em, nil
}

func (e *ExecutionManager) am() *AbstractManager {
	return (e.Manager).(*AbstractManager)
}

func (e *ExecutionManager) engine() *Engine {
	return e.am().Engine()
}

func (e *ExecutionManager) replyCh() chan Reply {
	return e.am().ReplyCh()
}

func (e *ExecutionManager) preLoop() error {
	eng := e.engine()
	rc := e.replyCh()

	e.id = eng.NextRequestID()
	eng.Subscribe(rc, e.id)
	req := &RequestExecutions{Filter: e.filter}
	req.SetID(e.id)
	return eng.Send(req)
}

func (e *ExecutionManager) receive(r Reply) (UpdateStatus, error) {
	switch r.(type) {
	case *ErrorMessage:
		r := r.(*ErrorMessage)
		if r.SeverityWarning() {
			return UpdateFalse, nil
		}
		return UpdateFalse, r
	case *ExecutionData:
		t := r.(*ExecutionData)
		e.values = append(e.values, *t)
		return UpdateFalse, nil
	case *ExecutionDataEnd:
		return UpdateFinish, nil
	}
	return UpdateFalse, fmt.Errorf("Unexpected type %v", r)
}

func (e *ExecutionManager) preDestroy() {
	e.engine().Unsubscribe(e.replyCh(), e.id)
}

// Values returns the most recent snapshot of execution information.
func (e *ExecutionManager) Values() []ExecutionData {
	am := e.am()
	am.RLock()
	defer am.RUnlock()
	return e.values
}
