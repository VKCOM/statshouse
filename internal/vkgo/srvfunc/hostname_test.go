// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"testing"
)

func Test_Name10(t *testing.T) {
	var str string
	var ok bool
	str, ok = removeKuberDeploymentName("glavred-production-mltasks-5649b7d9b4-d5qtf")
	if !ok || str != "glavred-production-mltasks-5649b7d9b4" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("keanu-rq-production-mltasks-d665549fb-6phdc")
	if !ok || str != "keanu-rq-production-mltasks-d665549fb" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("-d665549fb-6phdc")
	if ok || str != "-d665549fb-6phdc" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("a-d665549fb-6phdc")
	if !ok || str != "a-d665549fb" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("-5649b7d9b4-d5qtf")
	if ok || str != "-5649b7d9b4-d5qtf" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("a-5649b7d9b4-d5qtf")
	if !ok || str != "a-5649b7d9b4" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("app315908")
	if ok || str != "app315908" {
		t.Fatalf("Bad kuber check")
	}
	str, ok = removeKuberDeploymentName("keanu-rq-production-mltasks-d665549fb")
	if ok || str != "keanu-rq-production-mltasks-d665549fb" {
		t.Fatalf("Bad kuber check")
	}
}
