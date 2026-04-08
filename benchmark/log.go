package benchmark

import (
	"fmt"
	"os"
	"time"
)

func logProgress(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}
