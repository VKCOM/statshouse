// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Generate from old binlog implementation
var expectedHeader = `{"Magic":75123787,"Format":16777362,"OrigFileSize":1073742124,"OrigFileMd5":[147,101,224,134,67,218,60,162,56,125,87,144,105,183,145,93],"First36Bytes":[75,100,76,4,222,204,225,240,0,0,0,0,64,0,0,0,16,0,0,0,17,0,0,0,84,97,71,4,64,139,194,173,150,154,98,119],"Last36Bytes":[114,76,70,4,128,59,209,98,44,1,0,64,0,0,0,0,56,156,10,16,4,173,34,45,1,119,207,78,231,177,64,16,7,78,171,202],"First1mMd5":[193,103,19,46,78,156,211,139,80,151,35,126,241,112,120,215],"First128kMd5":[64,139,194,173,150,154,98,119,2,164,185,106,144,189,151,87],"Last128kMd5":[41,34,28,9,64,118,16,76,129,153,122,79,103,79,198,17],"FileHash":5678888502073404676,"ChunkOffset":[684,1442884,2722800,4501304,6212420,7699596,8974012,10260832,11531616,12873216,14141684,15372660,16697356,18034328,19342500,20660056,22027968,23448036,24887500,26241080,27565048,28950788,30368064,31738916,33040300,34478328,35820264,37224500,38387764,39677616,41117712,42531288,43694608,44876396,46106788,47303276,48504372,49706720,50943472,52162096,53372932,54575804,55782184,56967048,58226656,59473392,60693412,61882000,63103892,64306600,65474868,66725032,67965484,69162644,70369588,71589936,72796616,73987872,75167436,76345400,77554976,78750316,79862592,81031092,82179396],"Crc32":1132763844}`

func TestReadCompressedHeader(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testFile := filepath.Join(filepath.Dir(filename), "test_data/compressed/header.lev")

	data, err := os.ReadFile(testFile)
	require.NoError(t, err)

	var lev kfsBinlogZipHeader
	n, err := readKfsBinlogZipHeader(&lev, data)
	require.NoError(t, err)
	_ = n

	var levExpected kfsBinlogZipHeader
	err = json.Unmarshal([]byte(expectedHeader), &levExpected)
	require.NoError(t, err)

	assert.Equal(t, levExpected, lev)
}
