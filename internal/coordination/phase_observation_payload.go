package coordination

func samePhaseObservationPayload(left, right phaseObservation) bool {
	return left.ID == right.ID &&
		left.Kind == right.Kind &&
		left.CommandID == right.CommandID &&
		left.Endpoint == right.Endpoint &&
		left.Generation == right.Generation &&
		left.BindingRef == right.BindingRef &&
		left.LeaseRef == right.LeaseRef &&
		left.CheckpointRef == right.CheckpointRef &&
		left.NonResumable == right.NonResumable &&
		left.DispatchError == right.DispatchError &&
		left.DispatchTarget == right.DispatchTarget
}
