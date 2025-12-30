/*
Copyright (c) 2008-2009, Shlomi Noach
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    * Neither the name of the organization nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	// Build the binary before running tests
	cmd := exec.Command("go", "build", "-o", "../../bin/go-chunk-update", ".")
	if err := cmd.Run(); err != nil {
		panic("Failed to build binary for testing: " + err.Error())
	}
	os.Exit(m.Run())
}

func TestHelp(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update", "--help")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, "A Go port of oak-chunk-update") {
		t.Errorf("Help output missing expected text. Got: %s", outputStr)
	}
	if !strings.Contains(outputStr, "Usage:") {
		t.Errorf("Help output missing usage. Got: %s", outputStr)
	}
}

func TestNoExecuteOption(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update", "--database", "test")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, "--execute is required") {
		t.Errorf("Expected error message not found. Got: %s", outputStr)
	}
}

func TestQueryWithoutOakChunk(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update", "--execute", "UPDATE test SET col=1", "--database", "test")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Expected error message not found. Got: %s", outputStr)
	}
}

func TestBasicChunking(t *testing.T) {
	// This test expects to fail since no database/table exists
	cmd := exec.Command("../../bin/go-chunk-update", "--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)", "--database", "nonexistent", "--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail due to missing database/table")
	}

	outputStr := string(output)
	// Should fail with DB connection error or table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}

func TestWithStartAndEndRange(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update",
		"--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)",
		"--database", "test",
		"--start-with", "1",
		"--end-with", "100",
		"--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	// Should fail with DB/table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}

func TestForceChunkingColumn(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update",
		"--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)",
		"--database", "test",
		"--force-chunking-column", "id",
		"--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	// Should fail with DB/table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}

func TestWithSleepBetweenChunks(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update",
		"--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)",
		"--database", "test",
		"--sleep", "100",
		"--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	// Should fail with DB/table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}

func TestVerboseOutput(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update",
		"--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)",
		"--database", "test",
		"--verbose",
		"--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	// Should fail with DB/table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}

func TestNoLogBin(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update",
		"--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)",
		"--database", "test",
		"--no-log-bin",
		"--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	// Should fail with DB/table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}

func TestSkipLockTables(t *testing.T) {
	cmd := exec.Command("../../bin/go-chunk-update",
		"--execute", "UPDATE test SET col=1 WHERE GO_CHUNK(test)",
		"--database", "test",
		"--skip-lock-tables",
		"--defaults-file", os.Getenv("HOME")+"/.my.cnf")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("Expected command to fail")
	}

	outputStr := string(output)
	// Should fail with DB/table error, not query error
	if strings.Contains(outputStr, "Query must contain GO_CHUNK(table_name)") {
		t.Errorf("Unexpected query error. Got: %s", outputStr)
	}
}
