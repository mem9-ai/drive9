package schema

import (
	"strings"
	"testing"
)

func TestFSLayerEventIDSchemaAndTiDBWideningRepair(t *testing.T) {
	spec, err := tidbSchemaSpecFromStatements(FSLayerTiDBSchemaStatements())
	if err != nil {
		t.Fatalf("tidbSchemaSpecFromStatements: %v", err)
	}
	table := mustTableSpecFromSchemaSpec(t, spec, "fs_layer_events")
	eventID := table.columns["event_id"]
	if eventID.columnType != "varchar(128)" {
		t.Fatalf("event_id column type=%q, want varchar(128)", eventID.columnType)
	}
	if eventID.modifySQL != "ALTER TABLE fs_layer_events MODIFY COLUMN event_id VARCHAR(128) NOT NULL" {
		t.Fatalf("event_id modify SQL=%q", eventID.modifySQL)
	}

	diffs := []tidbSchemaDiff{{
		kind:       tidbSchemaDiffColumnType,
		tableName:  "fs_layer_events",
		columnName: "event_id",
		repairSQL:  eventID.modifySQL,
	}}
	repairs := plannedTiDBSchemaRepairs(diffs)
	if len(repairs) != 1 || repairs[0] != eventID.modifySQL {
		t.Fatalf("planned repairs=%#v, want event_id widening", repairs)
	}
}

func TestFSLayerDB9SchemaIncludesEventIDWideningMigration(t *testing.T) {
	var createFound bool
	var migrationFound bool
	for _, statement := range FSLayerDB9SchemaStatements() {
		normalized := normalizeSQLFragment(statement)
		if strings.HasPrefix(normalized, "create table if not exists fs_layer_events ") && strings.Contains(normalized, "event_id varchar(128) not null") {
			createFound = true
		}
		if normalized == "alter table if exists fs_layer_events alter column event_id type varchar(128)" {
			migrationFound = true
		}
	}
	if !createFound {
		t.Fatal("db9 fs_layer_events create statement does not use VARCHAR(128)")
	}
	if !migrationFound {
		t.Fatal("db9 schema does not widen existing fs_layer_events.event_id")
	}
}
