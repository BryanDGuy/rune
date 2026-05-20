package storage

import "context"

func (s *BadgerStore) maintenanceLoop(ctx context.Context) {
	defer s.wg.Done()
	<-ctx.Done()
}
