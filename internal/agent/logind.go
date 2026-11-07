package agent

import (
	"context"

	"github.com/godbus/dbus/v5"
)

// DefaultSystemBus is where the host's D-Bus socket is mounted in the agent pod.
const DefaultSystemBus = "unix:path=/run/dbus/system_bus_socket"

// LogindPowerOff returns a function that asks systemd-logind on the host to power off.
func LogindPowerOff(address string) func(context.Context) error {
	return logind(address, "PowerOff")
}

// LogindSuspend returns a function that asks systemd-logind on the host to suspend to RAM.
func LogindSuspend(address string) func(context.Context) error {
	return logind(address, "Suspend")
}

func logind(address, method string) func(context.Context) error {
	return func(ctx context.Context) error {
		conn, err := dbus.Connect(address, dbus.WithContext(ctx))
		if err != nil {
			return err
		}
		defer conn.Close()
		obj := conn.Object("org.freedesktop.login1", "/org/freedesktop/login1")
		return obj.CallWithContext(ctx, "org.freedesktop.login1.Manager."+method, 0, false).Err
	}
}
