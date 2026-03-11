// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metarqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

const maxResponseBody = 16_000_000 // some protection
const pretty = true                // for debug
const printQueries = false

func (l *RQLiteLoader) sendRequest(address string, endpoint string, transaction bool, reqBody string) ([]byte, error) {
	if printQueries {
		fmt.Printf("rqlite: request is: %v\n", reqBody)
	}
	query := fmt.Sprintf("/db/%s?timings", endpoint)
	if pretty {
		query = query + "&pretty"
	}
	if transaction {
		query = query + "&transaction"
	}
	URL := "http://" + string(address) + query
	//if cred != nil {
	//	URL = "https://" + cred.Host + query
	//}
	req, err := http.NewRequest("POST", URL, strings.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	//if cred != nil {
	//	req.SetBasicAuth(cred.UserName, cred.Password)
	//}
	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("metarqlite: bad http status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	if resp.ContentLength > maxResponseBody {
		return nil, fmt.Errorf("metarqlite: body too large: %d", resp.ContentLength)
	}
	//if resp.ContentLength < 0 {
	//	return nil, fmt.Errorf("metarqlite: no content length")
	//}
	limitedReader := io.LimitReader(resp.Body, maxResponseBody)
	respBody, err := io.ReadAll(limitedReader)
	if err != nil {
		fmt.Printf("rqlite: response error is: %v\n", err)
		return nil, err
	}
	if printQueries {
		fmt.Printf("rqlite: response is: %v\n", string(respBody))
	}
	return respBody, nil
}

func (l *RQLiteLoader) makeBody(statements []string, args string) string {
	var str strings.Builder
	str.WriteString("[\n")
	for i, statement := range statements {
		_, _ = fmt.Fprintf(&str, "    [%q, %s]", strings.ReplaceAll(statement, "\n", " "), args)
		if i != len(statements)-1 {
			str.WriteString(",\n")
		} else {
			str.WriteString("\n")
		}
	}
	str.WriteString("]")
	return str.String()
}

func jtop(respBody []byte) (top map[string]any) {
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	decoder.UseNumber() // Configure the decoder to use json.Number

	if err := decoder.Decode(&top); err != nil {
		panic(fmt.Errorf("error unmarshaling JSON: %w", err))
	}
	return top
}

func jarray(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}

func jobject(v any) map[string]any {
	return v.(map[string]any)
}

func jstring(v any) string {
	if v == nil {
		return ""
	}
	return v.(string)
}

func jmapping(v any) tlstatshouse.Mapping {
	vv := jarray(v)
	return tlstatshouse.Mapping{
		Str:   jstring(vv[1]),
		Value: jint32(vv[0]),
	}
}

func jevent(v any) tlmetadata.Event {
	vv := jarray(v)
	e := tlmetadata.Event{
		Id:         jint64(vv[0]),
		Name:       jstring(vv[1]),
		Version:    jint64(vv[2]),
		Data:       jstring(vv[3]),
		UpdateTime: juint32(vv[4]),
		EventType:  jint32(vv[5]),
		Unused:     juint32(vv[6]),
	}
	e.SetNamespaceId(jint64(vv[7]))
	e.SetMetadata(jstring(vv[8]))
	return e
}

func juint32(v any) uint32 {
	res, err := strconv.ParseUint(string(v.(json.Number)), 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(res)
}

func jint64(v any) int64 {
	res, err := strconv.ParseInt(string(v.(json.Number)), 10, 64)
	if err != nil {
		panic(err)
	}
	return res
}

func jint32(v any) int32 {
	res, err := strconv.ParseInt(string(v.(json.Number)), 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(res)
}
