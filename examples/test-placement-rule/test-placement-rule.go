// Copyright 2019 PingCAP, Inc.
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
	"encoding/hex"
	"flag"
	"fmt"
	. "github.com/tikv/client-go/table-placement-rule"
	"os"
)

var (
	pdAddressArgv   = flag.String("pd", os.Getenv("DEFAULT_TEST_PD_ADDRESS"), "pd address")
	groupIdArgv     = flag.String("group", "test-engine-group", "group ID")
	tableIDArgv     = flag.Int64("rule-table-id", 12345, "mock table id for placement rule")
	engineLabelArgv = flag.String("engine-label", "test_engine", "label of self-defined raftstore")
	replicaCount    = flag.Uint("replica-cnt", 3, "max replica count of this rule")
	delGroup        = flag.Bool("del-group", false, "set true to delete group by ID")
	delRule         = flag.Bool("del-rule", false, "set true to delete rule by ID")
)

func Main2() {
	flag.Parse()

	pdUrl := *pdAddressArgv
	groupId := *groupIdArgv
	tableID := *tableIDArgv

	ruleId := fmt.Sprintf("table-%d-r", tableID)
	var labelConstraints = []LabelConstraint{{"engine", "in", []string{*engineLabelArgv}}}
	startKey, endKey := MakeTableRecordStartEndKey(tableID)

	ShowGroups(pdUrl)
	if *delGroup {
		DelGroup(pdUrl, groupId)
	} else {
		SetGroup(pdUrl, groupId)
	}
	if *delRule {
		DelRule(pdUrl, groupId, ruleId)
	} else {
		SetRule(pdUrl, groupId, ruleId, hex.EncodeToString(startKey), hex.EncodeToString(endKey), int(*replicaCount), labelConstraints)
	}
}

func main() {
	Main2()
}
