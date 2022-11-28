// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package vkuth

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

const (
	vkuthJWTHeader       = "X-Vkuth-JWT"
	keyIDHeader          = "kid"
	JWTTimeWindow        = 5 * time.Second
	TokenIssuer          = "vkuth"
	KindHeaderName       = "kind"
	KindHeaderTokenValue = "token"
	VkuthKeyHeader       = "X-Vkuth-Key"
)

func ParseVkuthKeys(encoded []string) (map[string][]byte, error) {
	m := map[string][]byte{}
	for _, s := range encoded {
		k, err := parseVkuthKey(s)
		if err != nil {
			return nil, err
		}
		m[vkuthFingerprint(k)] = k
	}
	return m, nil
}

func ParseVkuthKeysPemInBase64(encoded []string) (map[string][]byte, error) {
	m := map[string][]byte{}
	for _, s := range encoded {
		k, err := parseVkuthKeyPemInBase64(s)
		if err != nil {
			return nil, err
		}
		m[vkuthFingerprint(k)] = k
	}
	return m, nil
}

func parseVkuthKeyPemInBase64(encoded string) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("unable to decode vkuth public key: %v", err)
	}
	publicKey, err := jwt.ParseEdPublicKeyFromPEM(b)

	if err != nil {
		return nil, fmt.Errorf("can't decode public key: %w", err)
	}
	keyBytes := publicKey.(ed25519.PublicKey)
	return keyBytes, nil
}

func parseVkuthKey(encoded string) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("unable to decode vkuth public key: %v", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("decoded vkuth public key must have length %v, not %v", ed25519.PublicKeySize, len(b))
	}
	return b, nil
}

func vkuthFingerprint(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:8])
}

func GetAccessToken(r *http.Request) string {
	return r.Header.Get(vkuthJWTHeader)
}

func stripFullBit(fullBit string, appName string) string {
	appPrefix := appName + ":"
	if !strings.HasPrefix(fullBit, appPrefix) {
		return ""
	}
	return fullBit[len(appPrefix):]
}

type Data struct {
	Bits []string `json:"bits"`
	User string   `json:"user"`
	VkID *int64   `json:"vk_id"`
}
type Claims struct {
	*jwt.RegisteredClaims
	Data    Data `json:"vkuth_data"`
	now     time.Time
	appName string
}

type JWTHelper struct {
	publicKeys map[string][]byte
	appName    string
	now        func() time.Time
}

type AccessData struct {
	Bits map[string]struct{} // bits without namespace and only for appName service
	User string
	VkID *int64
}

func NewJWTHelper(publicKeys map[string][]byte, appName string) *JWTHelper {
	return &JWTHelper{publicKeys: publicKeys, appName: appName, now: time.Now}
}

func (c *Claims) Valid() error {
	now := c.now
	expireAtNow := now.Add(-JWTTimeWindow)
	vErr := new(jwt.ValidationError)
	if !c.VerifyExpiresAt(expireAtNow, true) {
		delta := time.Unix(expireAtNow.Unix(), 0).Sub(time.Unix(c.ExpiresAt.Unix(), 0))
		vErr.Inner = fmt.Errorf("token is expired by %v", delta)
		vErr.Errors |= jwt.ValidationErrorExpired
	}

	if !c.VerifyIssuedAt(now.Add(JWTTimeWindow), true) {
		vErr.Inner = fmt.Errorf("token used before issued")
		vErr.Errors |= jwt.ValidationErrorIssuedAt
	}

	if !c.VerifyNotBefore(now, false) {
		vErr.Inner = fmt.Errorf("token is not valid yet")
		vErr.Errors |= jwt.ValidationErrorNotValidYet
	}

	if c.Issuer != TokenIssuer {
		vErr.Inner = fmt.Errorf("issuer is not valid")
		vErr.Errors |= jwt.ValidationErrorClaimsInvalid
	}

	if c.Data.User == "" {
		vErr.Inner = fmt.Errorf("user must be non empty")
		vErr.Errors |= jwt.ValidationErrorClaimsInvalid
	}

	if vErr.Errors == 0 {
		return nil
	}
	return vErr
}

func (helper *JWTHelper) SetNow(now func() time.Time) {
	helper.now = now
}

func (helper *JWTHelper) ParseVkuthData(accessToken string) (*AccessData, error) {
	claims := &Claims{now: helper.now(), appName: helper.appName}
	_, err := jwt.ParseWithClaims(accessToken, claims, func(t *jwt.Token) (interface{}, error) {
		kind, ok := t.Header[KindHeaderName]
		if !ok {
			return nil, fmt.Errorf("expect to get %s key in jwt header", KindHeaderName)
		}
		if kind != KindHeaderTokenValue {
			return nil, fmt.Errorf("exepct to get %s: %s in jwt header, but got %s", KindHeaderName, KindHeaderTokenValue, kind)
		}
		kid, ok := t.Header[keyIDHeader]
		kidStr, kidIsStr := kid.(string)
		if !ok || !kidIsStr {
			return nil, fmt.Errorf("expect to get %q: <string>, but got: %q", keyIDHeader, kid)
		}
		k, ok := helper.publicKeys[kidStr]
		if !ok {
			return nil, fmt.Errorf("unknown key ID %q", kidStr)
		}
		return ed25519.PublicKey(k), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}))
	if err != nil {
		return nil, fmt.Errorf("failed to verify access token: %w", err)
	}
	bits := map[string]struct{}{}
	for i := range claims.Data.Bits {
		nonNamespacedBit := stripFullBit(claims.Data.Bits[i], helper.appName)
		if nonNamespacedBit != "" {
			bits[nonNamespacedBit] = struct{}{}
		}
	}
	return &AccessData{
		Bits: bits,
		User: claims.Data.User,
		VkID: claims.Data.VkID,
	}, nil
}
