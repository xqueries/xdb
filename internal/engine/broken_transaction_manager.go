package engine

type brokenTransactionManager struct {
}

func (m *brokenTransactionManager) Close() error {
	return nil
}

func (m *brokenTransactionManager) Start() (*Transaction, error) {
	return &Transaction{}, nil
}

func (m *brokenTransactionManager) Submit(tx *Transaction) error {
	return nil
}
