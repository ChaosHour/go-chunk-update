/*
Copyright (c) 2008-2009, Shlomi Noach
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    * Neither the name of the organization nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package mysql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseMyCnf(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "mysql_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test .my.cnf file
	configContent := `[client]
user=testuser
password=testpass
host=testhost
port=3307
socket=/tmp/test.sock
database=testdb
`
	configFile := filepath.Join(tempDir, ".my.cnf")
	err = os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Test parsing
	config, err := parseMyCnf(configFile)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]string{
		"user":     "testuser",
		"password": "testpass",
		"host":     "testhost",
		"port":     "3307",
		"socket":   "/tmp/test.sock",
		"database": "testdb",
	}

	for key, expectedValue := range expected {
		if actualValue, ok := config[key]; !ok || actualValue != expectedValue {
			t.Errorf("Expected %s=%s, got %s=%s", key, expectedValue, key, actualValue)
		}
	}
}

func TestParseMyCnfDefault(t *testing.T) {
	// Test with empty string (uses ~/.my.cnf)
	_, err := parseMyCnf("")
	// It may or may not exist, so we don't assert error
	_ = err // just call it
}

func TestParseMyCnfNonExistent(t *testing.T) {
	_, err := parseMyCnf("/non/existent/file")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}
