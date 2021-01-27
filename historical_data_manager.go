package ib

import "fmt"

// HistoricalDataManager .
type HistoricalDataManager struct {
	Manager
	request  RequestHistoricalData
	histData []HistoricalDataItem
}

// NewHistoricalDataManager Create a new HistoricalDataManager for the given data request.
func NewHistoricalDataManager(e *Engine, request RequestHistoricalData) (*HistoricalDataManager, error) {
	am, err := NewAbstractManager(e)
	if err != nil {
		return nil, err
	}

	request.id = e.NextRequestID()
	m := &HistoricalDataManager{
		Manager: am,
		request: request,
	}

	go m.am().StartMainLoop(m.preLoop, m.receive, m.preDestroy)
	return m, nil
}

func (m *HistoricalDataManager) am() *AbstractManager {
	return (m.Manager).(*AbstractManager)
}

func (m *HistoricalDataManager) engine() *Engine {
	return m.am().Engine()
}

func (m *HistoricalDataManager) replyCh() chan Reply {
	return m.am().ReplyCh()
}

func (m *HistoricalDataManager) preLoop() error {
	eng := m.engine()
	rc := m.replyCh()
	eng.Subscribe(rc, m.request.id)
	return eng.Send(&m.request)
}

func (m *HistoricalDataManager) receive(r Reply) (UpdateStatus, error) {
	switch r.(type) {
	case *ErrorMessage:
		r := r.(*ErrorMessage)
		if r.SeverityWarning() {
			return UpdateFalse, nil
		}
		return UpdateFalse, r
	case *HistoricalData:
		hd := r.(*HistoricalData)
		m.histData = hd.Data
		return UpdateFinish, nil
	}
	return UpdateFalse, fmt.Errorf("Unexpected type %v", r)
}

func (m *HistoricalDataManager) preDestroy() {
	eng := m.engine()
	rc := m.replyCh()
	eng.Unsubscribe(rc, m.request.id)
}

// Items .
func (m *HistoricalDataManager) Items() []HistoricalDataItem {
	am := m.am()
	am.RLock()
	defer am.RUnlock()
	return m.histData
}
