package kernel

import "fmt"

// Revision is a monotonic optimistic-concurrency token. Revision zero is
// reserved for "latest" reads; writes must use a concrete non-zero revision.
type Revision uint64

const LatestRevision Revision = 0

func (r Revision) IsLatestRead() bool {
	return r == LatestRevision
}

func (r Revision) Next() Revision {
	return r + 1
}

func CheckExpectedRevision(expected, actual Revision) error {
	if expected == LatestRevision {
		return RevisionConflict(expected, actual)
	}
	if expected != actual {
		return RevisionConflict(expected, actual)
	}
	return nil
}

func RevisionConflict(expected, actual Revision) error {
	return Error{
		Code:        CodeRevisionConflict,
		Message:     fmt.Sprintf("revision conflict: expected %d, actual %d", expected, actual),
		Recoverable: true,
		Details: map[string]string{
			"expected_revision": fmt.Sprintf("%d", expected),
			"actual_revision":   fmt.Sprintf("%d", actual),
		},
	}
}
