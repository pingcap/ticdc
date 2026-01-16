package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// Motif steps:
// 0: initial
// 1: site_code column added with per-db defaults
// 2: site_code default unified to empty string
// 3: primary key evolved to (a, site_code)
func runPrimaryMotif(
	ctx context.Context,
	db *sql.DB,
	model *clusterModel,
	motifStep *int32,
	trace *ddlTrace,
	logger *log.Logger,
	profile string,
) {
	// The motif is a deterministic DDL sequence on a single table family (t03) intended to
	// cover tricky replication cases:
	//   - adding a new NOT NULL column with different defaults per database
	//   - unifying defaults over time
	//   - evolving primary keys after data already exists
	//
	// DML workers consult motifStep to adjust their write patterns accordingly.
	step1At, step2At, step3At := motifSchedule(profile)

	if err := sleepWithContext(ctx, step1At); err != nil {
		return
	}
	if err := motifAddSiteCode(ctx, db, model, trace, logger); err == nil {
		atomic.StoreInt32(motifStep, 1)
	}

	if err := sleepWithContext(ctx, step2At-step1At); err != nil {
		return
	}
	if err := motifUnifySiteCodeDefault(ctx, db, model, trace, logger); err == nil {
		atomic.StoreInt32(motifStep, 2)
	}

	if err := sleepWithContext(ctx, step3At-step2At); err != nil {
		return
	}
	if err := motifAddCompositePK(ctx, db, model, trace, logger); err == nil {
		atomic.StoreInt32(motifStep, 3)
	}
}

func motifSchedule(profile string) (time.Duration, time.Duration, time.Duration) {
	// Use a profile-based schedule so that smoke runs complete all steps quickly,
	// while weekly runs keep more steady-state time between transitions.
	if profile == "weekly" {
		return 2 * time.Minute, 10 * time.Minute, 20 * time.Minute
	}
	// Smoke mode: ensure all steps execute within a short run.
	return 10 * time.Second, 40 * time.Second, 70 * time.Second
}

func motifAddSiteCode(ctx context.Context, db *sql.DB, model *clusterModel, trace *ddlTrace, logger *log.Logger) error {
	for i, dbName := range model.dbs {
		defaultVal := fmt.Sprintf("%d", (i+1)*100)
		sqlText := fmt.Sprintf("ALTER TABLE `%s`.`t03` ADD COLUMN `site_code` VARCHAR(64) NOT NULL DEFAULT '%s'",
			dbName, defaultVal)
		_, err := db.ExecContext(ctx, sqlText)
		if trace != nil {
			trace.record("motif_add_site_code", fmt.Sprintf("`%s`.`t03`", dbName), sqlText, err)
		}
		if err != nil {
			if logger != nil {
				logger.Printf("motif step1 failed: db=%s err=%v", dbName, err)
			}
			return err
		}
		updateMotifSchemaAfterAdd(dbName, model, defaultVal)
	}
	return nil
}

func motifUnifySiteCodeDefault(ctx context.Context, db *sql.DB, model *clusterModel, trace *ddlTrace, logger *log.Logger) error {
	for _, dbName := range model.dbs {
		sqlText := fmt.Sprintf("ALTER TABLE `%s`.`t03` MODIFY COLUMN `site_code` VARCHAR(64) NOT NULL DEFAULT ''", dbName)
		_, err := db.ExecContext(ctx, sqlText)
		if trace != nil {
			trace.record("motif_unify_site_code_default", fmt.Sprintf("`%s`.`t03`", dbName), sqlText, err)
		}
		if err != nil {
			if logger != nil {
				logger.Printf("motif step2 failed: db=%s err=%v", dbName, err)
			}
			return err
		}
		updateMotifSchemaAfterUnify(dbName, model)
	}
	return nil
}

func motifAddCompositePK(ctx context.Context, db *sql.DB, model *clusterModel, trace *ddlTrace, logger *log.Logger) error {
	for _, dbName := range model.dbs {
		sqlText := fmt.Sprintf("ALTER TABLE `%s`.`t03` ADD PRIMARY KEY (`a`,`site_code`)", dbName)
		_, err := db.ExecContext(ctx, sqlText)
		if trace != nil {
			trace.record("motif_add_pk", fmt.Sprintf("`%s`.`t03`", dbName), sqlText, err)
		}
		if err != nil {
			if logger != nil {
				logger.Printf("motif step3 failed: db=%s err=%v", dbName, err)
			}
			return err
		}
		updateMotifSchemaAfterAddPK(dbName, model)
	}
	return nil
}

func updateMotifSchemaAfterAdd(dbName string, model *clusterModel, defaultVal string) {
	for _, t := range model.tables {
		if t.db != dbName || !t.isMotif || t.name != "t03" {
			continue
		}
		t.mu.Lock()
		t.schema.columns = append(t.schema.columns, column{
			name:       "site_code",
			typ:        colType{base: "VARCHAR", varcharN: 64},
			nullable:   false,
			defaultSQL: fmt.Sprintf("'%s'", defaultVal),
		})
		t.mu.Unlock()
	}
}

func updateMotifSchemaAfterUnify(dbName string, model *clusterModel) {
	for _, t := range model.tables {
		if t.db != dbName || !t.isMotif || t.name != "t03" {
			continue
		}
		t.mu.Lock()
		for i := range t.schema.columns {
			if t.schema.columns[i].name == "site_code" {
				t.schema.columns[i].defaultSQL = "''"
				break
			}
		}
		// Record the boundary so later updates can avoid touching frozen rows (which have non-empty site_code).
		t.motifUnifiedStart = t.nextID
		t.mu.Unlock()
	}
}

func updateMotifSchemaAfterAddPK(dbName string, model *clusterModel) {
	for _, t := range model.tables {
		if t.db != dbName || !t.isMotif || t.name != "t03" {
			continue
		}
		t.mu.Lock()
		t.schema.primaryKey = []string{"a", "site_code"}
		t.mu.Unlock()
	}
}
