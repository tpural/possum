package agent

import (
	"bufio"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

func bootTime() (time.Time, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "btime "); ok {
			secs, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return time.Time{}, err
			}
			return time.Unix(secs, 0), nil
		}
	}
	if err := sc.Err(); err != nil {
		return time.Time{}, err
	}
	return time.Time{}, errors.New("btime not found in /proc/stat")
}
