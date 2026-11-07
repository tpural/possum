package unifi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"sync"
	"time"

	"github.com/tpural/possum/pkg/power"
)

const (
	csrfHeader        = "X-Csrf-Token"
	updatedCSRFHeader = "X-Updated-Csrf-Token"
	maxAttempts       = 5
)

type client struct {
	base     string
	site     string
	username string
	password string
	http     *http.Client
	backoff  func(attempt int) time.Duration

	mu   sync.Mutex
	csrf string
}

func newClient(base, site, username, password string, insecure bool) (*client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecure} //nolint:gosec // opt-in for self-signed consoles
	return &client{
		base:     strings.TrimRight(base, "/"),
		site:     site,
		username: username,
		password: password,
		http:     &http.Client{Jar: jar, Transport: tr, Timeout: 20 * time.Second},
		backoff:  func(attempt int) time.Duration { return time.Duration(1<<attempt) * time.Second },
	}, nil
}

type envelope struct {
	Data json.RawMessage `json:"data"`
	Meta struct {
		RC  string `json:"rc"`
		Msg string `json:"msg"`
	} `json:"meta"`
}

func (c *client) network(ctx context.Context, method, path string, body, out any) error {
	var env envelope
	if err := c.do(ctx, method, "/proxy/network/api/s/"+c.site+path, body, &env); err != nil {
		return err
	}
	if env.Meta.RC != "" && env.Meta.RC != "ok" {
		return fmt.Errorf("unifi %s %s: %s", method, path, env.Meta.Msg)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(env.Data, out)
}

func (c *client) do(ctx context.Context, method, path string, body, out any) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, c.backoff(attempt)); err != nil {
				return err
			}
		}
		status, err := c.send(ctx, method, path, body, out)
		switch {
		case err == nil:
			return nil
		case status == http.StatusUnauthorized:
			if lerr := c.login(ctx); lerr != nil {
				lastErr = lerr
				if !errors.Is(lerr, power.ErrRateLimited) {
					return lerr
				}
			}
		case status == http.StatusTooManyRequests:
			lastErr = err
		default:
			return err
		}
	}
	return lastErr
}

func (c *client) send(ctx context.Context, method, path string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	c.mu.Lock()
	if c.csrf != "" {
		req.Header.Set(csrfHeader, c.csrf)
	}
	c.mu.Unlock()

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	c.rememberCSRF(resp.Header)

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusTooManyRequests:
		return resp.StatusCode, fmt.Errorf("unifi %s %s: %w", method, path, power.ErrRateLimited)
	default:
		return resp.StatusCode, fmt.Errorf("unifi %s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if out == nil {
		return resp.StatusCode, nil
	}
	return resp.StatusCode, json.NewDecoder(resp.Body).Decode(out)
}

func (c *client) login(ctx context.Context) error {
	creds := map[string]string{"username": c.username, "password": c.password}
	status, err := c.send(ctx, http.MethodPost, "/api/auth/login", creds, nil)
	if err != nil && status == http.StatusUnauthorized {
		return fmt.Errorf("unifi login rejected for %q", c.username)
	}
	return err
}

func (c *client) rememberCSRF(h http.Header) {
	token := h.Get(updatedCSRFHeader)
	if token == "" {
		token = h.Get(csrfHeader)
	}
	if token == "" {
		return
	}
	c.mu.Lock()
	c.csrf = token
	c.mu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
