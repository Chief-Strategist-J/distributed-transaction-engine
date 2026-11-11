package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
)

const minRSAKeyBits = 2048

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

func ParseJWKS(payload []byte) (map[string]crypto.PublicKey, error) {
	var set jwkSet
	if err := json.Unmarshal(payload, &set); err != nil {
		return nil, fmt.Errorf("jwks is not valid json: %w", err)
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, entry := range set.Keys {
		if err := addKey(keys, entry); err != nil {
			return nil, err
		}
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks contains no usable signing keys")
	}
	return keys, nil
}

func addKey(keys map[string]crypto.PublicKey, entry jwk) error {
	if entry.Kid == "" {
		return errors.New("jwks key is missing kid")
	}
	if _, duplicate := keys[entry.Kid]; duplicate {
		return fmt.Errorf("jwks contains duplicate kid %q", entry.Kid)
	}
	if entry.Use != "" && entry.Use != "sig" {
		return nil
	}
	key, err := buildKey(entry)
	if err != nil {
		return fmt.Errorf("jwks key %q rejected: %w", entry.Kid, err)
	}
	keys[entry.Kid] = key
	return nil
}

func buildKey(entry jwk) (crypto.PublicKey, error) {
	switch entry.Kty {
	case "RSA":
		return buildRSAKey(entry)
	case "EC":
		return buildECKey(entry)
	default:
		return nil, fmt.Errorf("unsupported key type %q", entry.Kty)
	}
}

func buildRSAKey(entry jwk) (crypto.PublicKey, error) {
	modulus, err := decodeBigInt(entry.N)
	if err != nil {
		return nil, err
	}
	exponent, err := decodeBigInt(entry.E)
	if err != nil || !exponent.IsInt64() || exponent.Int64() < 3 {
		return nil, errors.New("invalid rsa exponent")
	}
	if modulus.BitLen() < minRSAKeyBits {
		return nil, fmt.Errorf("rsa key shorter than %d bits", minRSAKeyBits)
	}
	return &rsa.PublicKey{N: modulus, E: int(exponent.Int64())}, nil
}

func buildECKey(entry jwk) (crypto.PublicKey, error) {
	curve, err := curveByName(entry.Crv)
	if err != nil {
		return nil, err
	}
	x, xErr := decodeBigInt(entry.X)
	y, yErr := decodeBigInt(entry.Y)
	if xErr != nil || yErr != nil || !curve.IsOnCurve(x, y) {
		return nil, errors.New("ec point is not on the declared curve")
	}
	return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
}

func curveByName(name string) (elliptic.Curve, error) {
	switch name {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("unsupported curve %q", name)
	}
}

func decodeBigInt(encoded string) (*big.Int, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 {
		return nil, errors.New("invalid base64url integer")
	}
	return new(big.Int).SetBytes(raw), nil
}
