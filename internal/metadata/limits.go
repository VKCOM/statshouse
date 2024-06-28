package metadata

import (
	"fmt"

	"github.com/vkcom/statshouse/internal/data_model"
)

const maxReqSize = 1024 * 1024

var errInvalidMetricVersion = fmt.Errorf("invalid version")
var errMetricIsExist = fmt.Errorf("entity is exists")
var errNamespaceNotExists = fmt.Errorf("namespace doesn't exists")

func checkLimit(req []byte) error {
	if len(req) > maxReqSize {
		return data_model.ErrRequestIsTooBig
	}
	return nil
}
