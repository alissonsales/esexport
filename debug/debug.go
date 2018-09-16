package debug

import "os"

type debugFunc func()

var shouldDebug bool

// Init turn the debug on if the given env variable is set
func Init(envVar string) {
	if os.Getenv(envVar) != "" {
		shouldDebug = true
	}
}

// Debug calls the given function if the debug environment variable is set
func Debug(f debugFunc) {
	if shouldDebug {
		f()
	}
}
