package ib

import (
	"fmt"
)

type optionm struct {
	sectype  string
	exchange string
}

// MetadataManager .
type MetadataManager struct {
	Manager
	id       int64
	c        Contract
	options  []optionm
	metadata []ContractData
}

// NewMetadataManager .
func NewMetadataManager(e *Engine, c Contract) (*MetadataManager, error) {
	am, err := NewAbstractManager(e)
	if err != nil {
		return nil, err
	}

	options := []optionm{
		{"", ""}, // send as per contract
		{"STK", "SMART"},
		{"IND", "SMART"},
		{"FUT", "GLOBEX"},
		{"IND", "DTB"},
		{"FUT", "DTB"},
	}

	m := &MetadataManager{
		Manager:  am,
		c:        c,
		metadata: []ContractData{},
		options:  options,
	}

	go m.am().StartMainLoop(m.preLoop, m.receive, m.preDestroy)
	return m, nil
}

func (m *MetadataManager) am() *AbstractManager {
	return (m.Manager).(*AbstractManager)
}

func (m *MetadataManager) engine() *Engine {
	return m.am().Engine()
}

func (m *MetadataManager) replyCh() chan Reply {
	return m.am().ReplyCh()
}

func (m *MetadataManager) preLoop() error {
	return m.request()
}

func (m *MetadataManager) request() error {
	if len(m.options) == 0 {
		return nil
	}

	opt := m.options[0]
	m.options = m.options[1:]

	if opt.sectype != "" {
		m.c.SecurityType = opt.sectype
	}

	if opt.exchange != "" {
		m.c.Exchange = opt.exchange
	}

	eng := m.engine()
	rc := m.replyCh()

	eng.Unsubscribe(rc, m.id) // AbstractMgr goroutine already rx reply
	m.id = eng.NextRequestID()
	req := &RequestContractData{
		Contract: m.c,
	}
	req.SetID(m.id)
	eng.Subscribe(rc, m.id)

	return eng.Send(req)
}

func (m *MetadataManager) preDestroy() {
	eng := m.engine()
	rc := m.replyCh()

	eng.Unsubscribe(rc, m.id)
}

func (m *MetadataManager) receive(r Reply) (UpdateStatus, error) {
	switch r.(type) {
	case *ErrorMessage:
		r := r.(*ErrorMessage)
		if r.Code == 321 || r.Code == 200 {
			if err := m.request(); err != nil {
				return UpdateFalse, err
			}
			return UpdateFalse, nil
		}
		if r.SeverityWarning() {
			return UpdateFalse, nil
		}
		return UpdateFalse, r
	case *ContractData:
		r := r.(*ContractData)
		m.metadata = append(m.metadata, *r)
		return UpdateFalse, nil
	case *ContractDataEnd:
		return UpdateFinish, nil
	}
	return UpdateFalse, fmt.Errorf("Unexpected type %v", r)
}

// Contract .
func (m *MetadataManager) Contract() Contract {
	am := m.am()
	am.RLock()
	defer am.RUnlock()
	return m.c
}

// ContractData .
func (m *MetadataManager) ContractData() []ContractData {
	am := m.am()
	am.RLock()
	defer am.RUnlock()
	return m.metadata
}
