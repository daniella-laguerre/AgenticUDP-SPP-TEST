package main

import (
	"strings"
	"testing"
)

// TestParsePathsFlag_Valid covers the inputs the writeup's
// run-protocol guidance documents as supported. Every variant
// here is something an operator will type at the PowerShell
// prompt; if any of these fails we'd be punishing the operator
// for a typing convention we encouraged.
func TestParsePathsFlag_Valid(t *testing.T) {
	cases := []struct {
		in   string
		want []string // sorted set of enabled keys
	}{
		{"http,grpc,udp", []string{"grpc", "http", "udp"}},
		{"http", []string{"http"}},
		{"grpc", []string{"grpc"}},
		{"udp", []string{"udp"}},
		{"http,udp", []string{"http", "udp"}},
		{"HTTP", []string{"http"}},                 // case insensitivity
		{"  http , grpc  ", []string{"grpc", "http"}}, // whitespace tolerance
		{"http,http,grpc", []string{"grpc", "http"}}, // dedup
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := parsePathsFlag(c.in)
			if err != nil {
				t.Fatalf("parsePathsFlag(%q) returned error: %v", c.in, err)
			}
			if len(got) != len(c.want) {
				t.Errorf("parsePathsFlag(%q): got %d keys, want %d (got=%v)", c.in, len(got), len(c.want), got)
			}
			for _, k := range c.want {
				if !got[k] {
					t.Errorf("parsePathsFlag(%q): missing expected key %q (got=%v)", c.in, k, got)
				}
			}
		})
	}
}

// TestParsePathsFlag_Invalid is the load-bearing test: before
// this change the bench tool would silently accept -paths foo,
// run zero paths, and exit 0 — operator wouldn't notice until
// the JSON came back with an empty results array. The writeup's
// run-protocol guidance leans on per-path invocations
// (-paths http etc.), so an unrecognized token is now a fatal
// flag-parse error.
func TestParsePathsFlag_Invalid(t *testing.T) {
	cases := []struct {
		in        string
		wantErrIn string
	}{
		{"", "empty value"},
		{"foo", "invalid path"},
		{"http,foo", "invalid path"},
		{"http,grpc,udp,quic", "invalid path"},
		{",,,", "empty value"}, // splitCSV drops empties → no tokens
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			_, err := parsePathsFlag(c.in)
			if err == nil {
				t.Fatalf("parsePathsFlag(%q): expected error, got nil", c.in)
			}
			if !strings.Contains(err.Error(), c.wantErrIn) {
				t.Errorf("parsePathsFlag(%q): error %q does not contain %q", c.in, err.Error(), c.wantErrIn)
			}
		})
	}
}

// TestParsePathsFlag_OnlyValidKeysReturned guarantees the call
// site in main() can rely on enabled["http"|"grpc"|"udp"] being
// the only keys that can ever be true. Without this guarantee a
// future merger that ranges over enabled would silently process
// stale tokens.
func TestParsePathsFlag_OnlyValidKeysReturned(t *testing.T) {
	got, err := parsePathsFlag("http,grpc,udp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for k := range got {
		if k != "http" && k != "grpc" && k != "udp" {
			t.Errorf("parsePathsFlag returned unexpected key %q", k)
		}
	}
}
