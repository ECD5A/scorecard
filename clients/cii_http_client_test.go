// Copyright 2026 OpenSSF Scorecard Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clients

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type ciiRoundTripFunc func(*http.Request) (*http.Response, error)

func (f ciiRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type ciiTrackingBody struct {
	io.Reader
	closeCount int
}

func (b *ciiTrackingBody) Close() error {
	b.closeCount++
	return nil
}

func setCIIHTTPTransport(t *testing.T, transport http.RoundTripper) {
	t.Helper()
	previousClient := http.DefaultClient
	t.Cleanup(func() { http.DefaultClient = previousClient })
	http.DefaultClient = &http.Client{Transport: transport}
}

//nolint:paralleltest // The test replaces http.DefaultClient.
func TestExpBackoffTransportCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testCIIBackoffCancellation(t, deadline)
			})
		})
	}
}

func testCIIBackoffCancellation(t *testing.T, deadline bool) {
	t.Helper()
	const delay = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	wantErr := context.Canceled
	if deadline {
		cancel()
		ctx, cancel = context.WithTimeout(t.Context(), delay)
		wantErr = context.DeadlineExceeded
	} else {
		go func() {
			time.Sleep(delay)
			cancel()
		}()
	}
	defer cancel()
	body := &ciiTrackingBody{Reader: strings.NewReader("retry later")}
	attempts := 0
	setCIIHTTPTransport(t, ciiRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		attempts++
		return &http.Response{StatusCode: http.StatusTooManyRequests, Body: body}, nil
	}))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://cii.example/projects.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := (&expBackoffTransport{numRetries: 3}).RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if resp != nil || !errors.Is(err, wantErr) {
		t.Fatalf("RoundTrip() = (%v, %v), want (nil, %v)", resp, err, wantErr)
	}
	if elapsed := time.Since(start); elapsed != delay {
		t.Errorf("backoff returned after %s, want %s", elapsed, delay)
	}
	if attempts != 1 || body.closeCount != 1 {
		t.Errorf("attempts = %d, body closes = %d; want 1 each", attempts, body.closeCount)
	}
}

type ciiResponseTest struct {
	transportErr error
	wantErr      error
	name         string
	statuses     []int
	wantElapsed  time.Duration
}

//nolint:paralleltest // The test replaces http.DefaultClient.
func TestExpBackoffTransportResponses(t *testing.T) {
	transportErr := errors.New("transport failed")
	for _, tt := range []ciiResponseTest{
		{name: "success", statuses: []int{http.StatusOK}},
		{name: "non-retryable response", statuses: []int{http.StatusServiceUnavailable}},
		{name: "success after retries", statuses: []int{http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusOK}, wantElapsed: 3 * time.Second},
		{name: "exhausted", statuses: []int{http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusTooManyRequests}, wantErr: errTooManyRequests, wantElapsed: 7 * time.Second},
		{name: "transport error", statuses: []int{0}, transportErr: transportErr, wantErr: transportErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testCIIBackoffResponse(t, &tt)
			})
		})
	}
}

func testCIIBackoffResponse(t *testing.T, tt *ciiResponseTest) {
	t.Helper()
	var bodies []*ciiTrackingBody
	attempts := 0
	setCIIHTTPTransport(t, ciiRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		for _, body := range bodies {
			if body.closeCount != 1 {
				t.Error("previous response body was not closed before retry")
			}
		}
		attempts++
		if attempts > len(tt.statuses) {
			t.Fatal("unexpected extra request")
		}
		if tt.transportErr != nil {
			return nil, tt.transportErr
		}
		body := &ciiTrackingBody{Reader: strings.NewReader("response")}
		bodies = append(bodies, body)
		return &http.Response{StatusCode: tt.statuses[attempts-1], Body: body}, nil
	}))
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://cii.example/projects.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := (&expBackoffTransport{numRetries: 3}).RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if !errors.Is(err, tt.wantErr) {
		t.Fatalf("RoundTrip() error = %v, want %v", err, tt.wantErr)
	}
	if tt.wantErr == nil {
		if resp == nil {
			t.Fatal("RoundTrip() returned no response")
		}
		if want := tt.statuses[len(tt.statuses)-1]; resp.StatusCode != want {
			t.Errorf("status = %d, want %d", resp.StatusCode, want)
		}
	} else if resp != nil {
		t.Errorf("RoundTrip() returned response with error: %v", resp)
	}
	if attempts != len(tt.statuses) || time.Since(start) != tt.wantElapsed {
		t.Errorf("attempts = %d, elapsed = %s; want %d, %s", attempts, time.Since(start), len(tt.statuses), tt.wantElapsed)
	}
	for _, body := range bodies {
		wantCloses := 1
		if resp != nil && resp.Body == body {
			wantCloses = 0
		}
		if body.closeCount != wantCloses {
			t.Errorf("body close count = %d, want %d", body.closeCount, wantCloses)
		}
	}
	if resp != nil {
		data, err := io.ReadAll(resp.Body)
		if err != nil || string(data) != "response" {
			t.Errorf("returned body = %q, error = %v", data, err)
		}
	}
}
