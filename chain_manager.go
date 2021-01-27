package ib

import (
	"fmt"
	"time"
)

// ChainManager .
type ChainManager struct {
	Manager
	id     int64
	c      Contract
	chains OptionChains
}

// NewChainManager .
func NewChainManager(e *Engine, c Contract) (*ChainManager, error) {
	am, err := NewAbstractManager(e)
	if err != nil {
		return nil, err
	}

	m := &ChainManager{
		Manager: am,
		c:       c,
		chains:  OptionChains{},
	}

	go m.am().StartMainLoop(m.preLoop, m.receive, m.preDestroy)
	return m, nil
}

func (c *ChainManager) am() *AbstractManager {
	return (c.Manager).(*AbstractManager)
}

func (c *ChainManager) engine() *Engine {
	return c.am().Engine()
}

func (c *ChainManager) replyCh() chan Reply {
	return c.am().ReplyCh()
}

func (c *ChainManager) preLoop() error {
	eng := c.engine()
	rc := c.replyCh()
	c.id = eng.NextRequestID()
	req := &RequestContractData{Contract: c.c}
	req.Contract.SecurityType = "OPT"
	req.Contract.LocalSymbol = ""
	req.SetID(c.id)
	eng.Subscribe(rc, c.id)
	return eng.Send(req)
}

func (c *ChainManager) preDestroy() {
	c.engine().Unsubscribe(c.replyCh(), c.id)
}

func (c *ChainManager) receive(r Reply) (UpdateStatus, error) {
	switch r.(type) {
	case *ErrorMessage:
		r := r.(*ErrorMessage)
		if r.SeverityWarning() {
			return UpdateFalse, nil
		}
		return UpdateFalse, r
	case *ContractData:
		r := r.(*ContractData)
		expiry, err := time.Parse("20060102", r.Contract.Summary.Expiry)
		if err != nil {
			return UpdateFalse, err
		}
		if _, ok := c.chains[expiry]; !ok {
			c.chains[expiry] = &OptionChain{
				Expiry:  expiry,
				Strikes: map[float64]*OptionStrike{},
			}
		}
		c.chains[expiry].update(r)
		return UpdateFalse, nil
	case *ContractDataEnd:
		return UpdateFinish, nil
	}
	return UpdateFalse, fmt.Errorf("Unexpected type %v", r)
}

// Chains .
func (c *ChainManager) Chains() map[time.Time]*OptionChain {
	am := c.am()
	am.RLock()
	defer am.RUnlock()
	return c.chains
}
