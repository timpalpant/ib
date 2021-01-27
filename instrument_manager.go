package ib

// InstrumentManager .
type InstrumentManager struct {
	Manager
	id   int64
	c    Contract
	last float64
	bid  float64
	ask  float64
}

// NewInstrumentManager .
func NewInstrumentManager(e *Engine, c Contract) (*InstrumentManager, error) {
	am, err := NewAbstractManager(e)
	if err != nil {
		return nil, err
	}

	m := &InstrumentManager{
		Manager: am,
		c:       c,
	}

	go m.am().StartMainLoop(m.preLoop, m.receive, m.preDestroy)
	return m, nil
}

func (i *InstrumentManager) am() *AbstractManager {
	return (i.Manager).(*AbstractManager)
}

func (i *InstrumentManager) engine() *Engine {
	return i.am().Engine()
}

func (i *InstrumentManager) replyCh() chan Reply {
	return i.am().ReplyCh()
}

func (i *InstrumentManager) preLoop() error {
	eng := i.engine()
	rc := i.replyCh()

	i.id = eng.NextRequestID()
	req := &RequestMarketData{Contract: i.c}
	req.SetID(i.id)
	eng.Subscribe(rc, i.id)
	return eng.Send(req)
}

func (i *InstrumentManager) preDestroy() {
	eng := i.engine()
	rc := i.replyCh()

	eng.Unsubscribe(rc, i.id)
	req := &CancelMarketData{}
	req.SetID(i.id)
	eng.Send(req)
}

func (i *InstrumentManager) receive(r Reply) (UpdateStatus, error) {
	switch r.(type) {
	case *ErrorMessage:
		r := r.(*ErrorMessage)
		if r.SeverityWarning() {
			return UpdateFalse, nil
		}
		return UpdateFalse, r
	case *TickPrice:
		r := r.(*TickPrice)
		switch r.Type {
		case TickLast:
			i.last = r.Price
		case TickBid:
			i.bid = r.Price
		case TickAsk:
			i.ask = r.Price
		}
	}

	if i.last <= 0 && (i.bid <= 0 || i.ask <= 0) {
		return UpdateFalse, nil
	}
	return UpdateTrue, nil
}

// Bid .
func (i *InstrumentManager) Bid() float64 {
	am := i.am()
	am.RLock()
	defer am.RUnlock()
	return i.bid
}

// Ask .
func (i *InstrumentManager) Ask() float64 {
	am := i.am()
	am.RLock()
	defer am.RUnlock()
	return i.ask
}

// Last .
func (i *InstrumentManager) Last() float64 {
	am := i.am()
	am.RLock()
	defer am.RUnlock()
	return i.last
}
