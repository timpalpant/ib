package ib

import "time"

// CurrentTimeManager provides a Manager to access the IB current time on the server side
type CurrentTimeManager struct {
	Manager
	id int64
	t  time.Time
}

// NewCurrentTimeManager .
func NewCurrentTimeManager(e *Engine) (*CurrentTimeManager, error) {
	am, err := NewAbstractManager(e)
	if err != nil {
		return nil, err
	}

	m := &CurrentTimeManager{Manager: am, id: UnmatchedReplyID}

	go m.am().StartMainLoop(m.preLoop, m.receive, m.preDestroy)
	return m, nil
}

func (m *CurrentTimeManager) am() *AbstractManager {
	return (m.Manager).(*AbstractManager)
}

func (m *CurrentTimeManager) engine() *Engine {
	return m.am().Engine()
}

func (m *CurrentTimeManager) replyCh() chan Reply {
	return m.am().ReplyCh()
}

func (m *CurrentTimeManager) preLoop() error {
	req := &RequestCurrentTime{}
	eng := m.engine()
	rc := m.replyCh()

	eng.Subscribe(rc, m.id)
	return eng.Send(req)
}

func (m *CurrentTimeManager) receive(r Reply) (UpdateStatus, error) {
	switch r.(type) {
	case *ErrorMessage:
		r := r.(*ErrorMessage)
		if r.SeverityWarning() {
			return UpdateFalse, nil
		}
		return UpdateFalse, r
	case *CurrentTime:
		ct := r.(*CurrentTime)
		m.t = ct.Time
		return UpdateFinish, nil
	}
	return UpdateFalse, nil
}

func (m *CurrentTimeManager) preDestroy() {
	m.engine().Unsubscribe(m.replyCh(), m.id)
}

// Time returns the current server time.
func (m *CurrentTimeManager) Time() time.Time {
	am := m.am()
	am.RLock()
	defer am.RUnlock()
	return m.t
}
