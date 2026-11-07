package gate

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strings"
	"time"
)

// Rule keeps the backend awake for Idle after a request it matches.
type Rule struct {
	Spec  string
	Idle  time.Duration
	match func(*http.Request) bool
}

// Matches reports whether r is covered by the rule.
func (r Rule) Matches(req *http.Request) bool { return r.match(req) }

// ParseRule parses "<kind> <pattern> <duration>" where kind is
//
//	host    the request's Host, without port (e.g. "llm.lan.example.com")
//	cidr    the client address (e.g. "10.244.0.0/16")
//	header  "<Name>=<regexp>" matched against a request header (e.g. "User-Agent=^n8n")
func ParseRule(spec string) (Rule, error) {
	f := strings.Fields(spec)
	if len(f) != 3 {
		return Rule{}, fmt.Errorf("idle rule %q: want <kind> <pattern> <duration>", spec)
	}
	idle, err := time.ParseDuration(f[2])
	if err != nil || idle <= 0 {
		return Rule{}, fmt.Errorf("idle rule %q: bad duration", spec)
	}
	r := Rule{Spec: spec, Idle: idle}
	switch kind, pattern := f[0], f[1]; kind {
	case "host":
		r.match = func(req *http.Request) bool { return strings.EqualFold(hostOnly(req.Host), pattern) }
	case "cidr":
		prefix, err := netip.ParsePrefix(pattern)
		if err != nil {
			return Rule{}, fmt.Errorf("idle rule %q: %w", spec, err)
		}
		r.match = func(req *http.Request) bool {
			ip, err := netip.ParseAddr(hostOnly(req.RemoteAddr))
			return err == nil && prefix.Contains(ip.Unmap())
		}
	case "header":
		name, expr, ok := strings.Cut(pattern, "=")
		if !ok || name == "" {
			return Rule{}, fmt.Errorf("idle rule %q: header pattern must be <Name>=<regexp>", spec)
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return Rule{}, fmt.Errorf("idle rule %q: %w", spec, err)
		}
		r.match = func(req *http.Request) bool { return re.MatchString(req.Header.Get(name)) }
	default:
		return Rule{}, fmt.Errorf("idle rule %q: unknown kind %q (host, cidr, header)", spec, kind)
	}
	return r, nil
}

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}
