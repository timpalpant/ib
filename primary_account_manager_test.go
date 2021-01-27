package ib

import (
	"testing"
	"time"
)

func TestPrimaryAccountManager(t *testing.T) {
	engine := NewTestEngine(t)

	defer engine.ConditionalStop(t)

	pam, err := NewPrimaryAccountManager(engine)
	if err != nil {
		t.Fatalf("error creating AccountManager, %v", err)
	}

	defer pam.Close()

	SinkManagerTest(t, pam, 15*time.Second, 1)

	if len(pam.Values()) < 3 {
		t.Fatalf("Insufficient account values %v", pam.Values())
	}

	// demo accounts have no guaranteed portfolio, so this just tests the accessor
	pam.Portfolio()

	if ack, ok := <-pam.Refresh(); ok {
		ack.Acknowledge()
		t.Fatalf("Expected the refresh channel to be closed, but got %t", ack)
	}
}
