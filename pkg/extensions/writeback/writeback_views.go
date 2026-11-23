package writeback

// viewLossWitnessLimit bounds the fallback witness, mirroring the commit
// queue's terminal-failure bound: eviction can only cost an already-lost image
// its second failure report, never the image itself.
const viewLossWitnessLimit = 1024

// recordViewLoss files a pruned staged pair: the commit queue's terminal-failure
// map when one exists, otherwise the bounded in-process witness.
func (w *Writeback) recordViewLoss(path string, err error) {
	if w == nil {
		return
	}
	if w.queue != nil {
		w.queue.RecordTerminalFailureForPath(path, err)
		return
	}
	w.viewLossMu.Lock()
	defer w.viewLossMu.Unlock()
	if w.viewLossWitness == nil {
		w.viewLossWitness = make(map[string]error)
	}
	if _, ok := w.viewLossWitness[path]; !ok {
		w.viewLossOrder = append(w.viewLossOrder, path)
	}
	w.viewLossWitness[path] = err
	for len(w.viewLossOrder) > viewLossWitnessLimit {
		oldest := w.viewLossOrder[0]
		w.viewLossOrder = w.viewLossOrder[1:]
		delete(w.viewLossWitness, oldest)
	}
}

// viewLoss returns the recorded loss for a path, if any.
func (w *Writeback) viewLoss(path string) error {
	if w == nil {
		return nil
	}
	w.viewLossMu.Lock()
	defer w.viewLossMu.Unlock()
	return w.viewLossWitness[path]
}

// clearViewLoss retires the witness after a landing or an explicit removal.
func (w *Writeback) clearViewLoss(path string) {
	if w == nil {
		return
	}
	w.viewLossMu.Lock()
	defer w.viewLossMu.Unlock()
	if _, ok := w.viewLossWitness[path]; !ok {
		return
	}
	delete(w.viewLossWitness, path)
	for i, p := range w.viewLossOrder {
		if p == path {
			w.viewLossOrder = append(w.viewLossOrder[:i], w.viewLossOrder[i+1:]...)
			break
		}
	}
}
