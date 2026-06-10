// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"workload/schema"
	"workload/schema/customerworkload"
)

func TestCreateCustomerWorkload(t *testing.T) {
	t.Parallel()

	cfg := NewWorkloadConfig()
	cfg.WorkloadType = customerWorkload
	cfg.CustomerModel = "D"
	cfg.TableCount = 2
	cfg.TotalRowCount = 1000

	app := NewWorkloadApp(cfg)
	workload := app.createWorkload()

	if _, ok := workload.(*customerworkload.CustomerWorkload); !ok {
		t.Fatalf("unexpected workload type %T", workload)
	}
	if _, ok := workload.(schema.InsertValuesWorkload); !ok {
		t.Fatalf("customer workload should support prepared inserts")
	}
	if _, ok := workload.(schema.UpdateValuesWorkload); !ok {
		t.Fatalf("customer workload should support prepared updates")
	}
	if _, ok := workload.(schema.DeleteValuesWorkload); !ok {
		t.Fatalf("customer workload should support prepared deletes")
	}
}

func TestCreateCustomerWorkloadRandomizesInsertInsideKeyspaceForWrite(t *testing.T) {
	t.Parallel()

	cfg := NewWorkloadConfig()
	cfg.WorkloadType = customerWorkload
	cfg.Action = "write"
	cfg.CustomerModel = "A"
	cfg.CustomerKeyspace = 100
	cfg.TableCount = 1

	app := NewWorkloadApp(cfg)
	workload := app.createWorkload().(schema.InsertValuesWorkload)

	_, values := workload.BuildInsertSqlWithValues(0, 8)
	for i := 0; i < len(values); i += 13 {
		entityID, ok := values[i].(uint64)
		if !ok {
			t.Fatalf("unexpected entity id type %T", values[i])
		}
		if entityID == 0 || entityID > 100 {
			t.Fatalf("expected write insert inside keyspace, got %d", entityID)
		}
	}
}
