package main

import "testing"

func TestDispatchRoutesRuntimeCommands(t *testing.T) {
	originalRuntime := runtimeHandler
	originalExec := runtimeExecHandler
	originalExecution := runtimeExecutionHandler
	originalFile := runtimeFileOperationHandler
	originalRecovery := runtimeRecoveryHandler
	t.Cleanup(func() {
		runtimeHandler = originalRuntime
		runtimeExecHandler = originalExec
		runtimeExecutionHandler = originalExecution
		runtimeFileOperationHandler = originalFile
		runtimeRecoveryHandler = originalRecovery
	})
	called := map[string]int{}
	runtimeHandler = func([]string) error { called["runtime"]++; return nil }
	runtimeExecHandler = func([]string) error { called["exec"]++; return nil }
	runtimeExecutionHandler = func([]string) error { called["execution"]++; return nil }
	runtimeFileOperationHandler = func([]string) error { called["file-operation"]++; return nil }
	runtimeRecoveryHandler = func([]string) error { called["recovery"]++; return nil }

	for _, command := range []string{"runtime", "exec", "execution", "file-operation", "recovery"} {
		dispatch(command, nil)
		if called[command] != 1 {
			t.Fatalf("%s handler calls = %d", command, called[command])
		}
	}
}
