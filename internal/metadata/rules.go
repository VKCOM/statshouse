package metadata

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/sqlite"
)

func resolveNamespace(c sqlite.Conn, name string, typ int32) (namespaceIDResolved int64, err error) {
	if typ != format.MetricEvent && typ != format.MetricsGroupEvent {
		return 0, nil
	}
	if !strings.Contains(name, format.NamespaceSeparator) {
		return 0, nil
	}
	_, namespaceName := format.SplitNamespace(name)
	r := c.Query("select_namespace", "SELECT id FROM metrics_v5 WHERE type = $type AND name = $namespaceName",
		sqlite.Int64("$type", int64(format.NamespaceEvent)),
		sqlite.TextString("$namespaceName", namespaceName))
	if !r.Next() {
		return 0, errors.Wrap(errNamespaceNotExists, fmt.Sprintf("namespace with name %s doesn't exists", namespaceName))
	}
	namespaceIDResolved, _ = r.ColumnInt64(0)
	return namespaceIDResolved, r.Error()
}

func checkCreateEntity(c sqlite.Conn, name string, typ int32) error {
	entityName := format.EventTypeToName(typ)
	r := c.Query("select_event", "SELECT id FROM metrics_v5 WHERE type = $type AND name = $name",
		sqlite.Int64("$type", int64(typ)),
		sqlite.TextString("$name", name))
	if r.Next() {
		return errors.Wrap(errMetricIsExist, fmt.Sprintf("%s %s is exists", entityName, name))
	}
	if r.Error() != nil {
		return r.Error()
	}
	return nil
}

func checkGroup(c sqlite.Conn, id int64, name string, createEntity bool) error {
	args := []sqlite.Arg{sqlite.Int64("$type", int64(format.MetricsGroupEvent))}
	r := c.Query("select_groups", "SELECT id, name FROM metrics_v5 WHERE type = $type", args...)
	for r.Next() {
		groupID, _ := r.ColumnInt64(0)
		groupName, err := r.ColumnBlobString(1)
		if err != nil {
			return err
		}
		if id == groupID {
			continue
		}
		if strings.HasPrefix(groupName, name) || strings.HasPrefix(name, groupName) {
			return fmt.Errorf("group can't be prefix of another group")
		}
	}
	return r.Error()
}

func checkNamespace(c sqlite.Conn, name string, id int64, oldVersion int64, newJson string, createEntity, deleteEntity bool) error {
	if !createEntity {
		oldName, err := loadNamespaceName(c, id, oldVersion)
		if err != nil {
			return err
		}
		if oldName != name {
			return fmt.Errorf("can't rename namespace")
		}
	}

	return nil
}

func resolveEntity(c sqlite.Conn, name string, id int64, oldVersion int64, newJson string, createEntity, deleteEntity bool, typ int32) (namespaceID int64, _ error) {
	var err error
	switch typ {
	case format.MetricsGroupEvent:
		err = checkGroup(c, id, name, createEntity)
	case format.NamespaceEvent:
		err = checkNamespace(c, name, id, oldVersion, newJson, createEntity, deleteEntity)
	}
	if err != nil {
		return 0, err
	}

	namespaceID, err = resolveNamespace(c, name, typ)
	if err != nil {
		return namespaceID, err
	}
	if createEntity {
		err := checkCreateEntity(c, name, typ)
		if err != nil {
			return namespaceID, err
		}
	}

	return namespaceID, nil
}
