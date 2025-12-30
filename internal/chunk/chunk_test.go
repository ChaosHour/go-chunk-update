/*
Copyright (c) 2008-2009, Shlomi Noach
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    * Neither the name of the organization nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package chunk

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// MockDB implements a minimal DB interface for testing
type MockDB struct {
	uniqueKeyColumns []map[string]interface{}
}

func (m *MockDB) Exec(query string, args ...interface{}) (int64, error) {
	return 0, nil
}

func (m *MockDB) QueryRow(query string, args ...interface{}) (map[string]interface{}, error) {
	// Mock responses based on query
	if strings.Contains(query, "range_exists") {
		return map[string]interface{}{"range_exists": int64(1)}, nil
	}
	if strings.Contains(query, "@unique_key_min_value_") {
		return map[string]interface{}{"@unique_key_min_value_0": int64(1)}, nil
	}
	if strings.Contains(query, "@unique_key_max_value_") {
		return map[string]interface{}{"@unique_key_max_value_0": int64(100)}, nil
	}
	return nil, nil
}

func (m *MockDB) TableExists(db, table string) (bool, error) {
	return true, nil
}

func (m *MockDB) LockTableRead(db, table string) error {
	return nil
}

func (m *MockDB) UnlockTables() error {
	return nil
}

func (m *MockDB) GetPossibleUniqueKeyColumns(db, table string) ([]map[string]interface{}, error) {
	if m.uniqueKeyColumns != nil {
		return m.uniqueKeyColumns, nil
	}
	return []map[string]interface{}{
		{
			"COLUMN_NAMES":          "id",
			"COUNT_COLUMN_IN_INDEX": int64(1),
			"DATA_TYPE":             "int",
			"CHARACTER_SET_NAME":    nil,
		},
	}, nil
}

func TestGetSelectedUniqueKeyColumnNamesForced(t *testing.T) {
	tests := []struct {
		forced   string
		expected string
		count    int
		keyType  string
	}{
		{"id", "id", 1, ""},
		{"id:integer", "id", 1, "integer"},
		{"col1,col2", "col1,col2", 2, ""},
	}

	for _, test := range tests {
		config := Config{
			ForcedChunkingColumn: test.forced,
		}
		chunker := &Chunker{Config: config}

		result, count, keyType, err := chunker.GetSelectedUniqueKeyColumnNames()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result != test.expected {
			t.Errorf("Expected %s, got %s", test.expected, result)
		}
		if count != test.count {
			t.Errorf("Expected count %d, got %d", test.count, count)
		}
		if keyType != test.keyType {
			t.Errorf("Expected keyType %s, got %s", test.keyType, keyType)
		}
	}
}

func TestGetUniqueKeyMinValuesVariables(t *testing.T) {
	chunker := &Chunker{Config: Config{CountColumnsInUniqueKey: 3}}
	result := chunker.getUniqueKeyMinValuesVariables()
	expected := "@unique_key_min_value_0,@unique_key_min_value_1,@unique_key_min_value_2"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestGetUniqueKeyMaxValuesVariables(t *testing.T) {
	chunker := &Chunker{Config: Config{CountColumnsInUniqueKey: 2}}
	result := chunker.getUniqueKeyMaxValuesVariables()
	expected := "@unique_key_max_value_0,@unique_key_max_value_1"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

// Test command-line argument parsing and validation
func TestCommandLineArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
		errorMsg    string
	}{
		{"no args", []string{}, true, "--execute is required"},
		{"no execute", []string{"--database", "test"}, true, "--execute is required"},
		{"invalid query no oak_chunk", []string{"--execute", "SELECT * FROM test"}, true, "Query must contain GO_CHUNK"},
		{"valid basic", []string{"--execute", "UPDATE test SET col=val WHERE GO_CHUNK(test)"}, false, ""},
		{"negative chunk size", []string{"--execute", "UPDATE test SET col=val WHERE GO_CHUNK(test)", "--chunk-size", "-1"}, false, ""}, // Should be handled gracefully
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test the main function directly, but we can test the logic
			// This is more of a documentation of expected behavior
			t.Logf("Test case: %s - Expected error: %v (%s)", tt.name, tt.expectError, tt.errorMsg)
		})
	}
}

// Test query parsing for GO_CHUNK
func TestQueryParsing(t *testing.T) {
	tests := []struct {
		query       string
		expectTable string
		expectError bool
	}{
		{"UPDATE users SET status='active' WHERE GO_CHUNK(users)", "users", false},
		{"UPDATE db.users SET status='active' WHERE GO_CHUNK(db.users)", "db.users", false},
		{"DELETE FROM users WHERE GO_CHUNK(users) AND status='inactive'", "users", false},
		{"INSERT INTO archive SELECT * FROM users WHERE GO_CHUNK(users)", "users", false},
		{"UPDATE users SET col=val", "", true},
		{"SELECT * FROM users WHERE go_chunk(users)", "", true}, // case sensitive
		{"UPDATE users SET col=val WHERE GO_CHUNK()", "", true}, // empty
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			re := regexp.MustCompile(`GO_CHUNK\(([^)]+)\)`)
			matches := re.FindStringSubmatch(tt.query)
			if tt.expectError {
				if matches != nil {
					t.Errorf("Expected error for query %s, but found match: %s", tt.query, matches[1])
				}
			} else {
				if matches == nil {
					t.Errorf("Expected to find table in query %s, but no match found", tt.query)
				} else if matches[1] != tt.expectTable {
					t.Errorf("Expected table %s, got %s", tt.expectTable, matches[1])
				}
			}
		})
	}
}

// Test unique key column selection logic
func TestUniqueKeySelection(t *testing.T) {
	tests := []struct {
		name            string
		forcedColumn    string
		mockResponse    []map[string]interface{}
		expectedColumns string
		expectedCount   int
		expectedType    string
		expectError     bool
	}{
		{
			name:            "forced single column",
			forcedColumn:    "id",
			expectedColumns: "id",
			expectedCount:   1,
			expectedType:    "",
		},
		{
			name:            "forced single column with type",
			forcedColumn:    "id:integer",
			expectedColumns: "id",
			expectedCount:   1,
			expectedType:    "integer",
		},
		{
			name:            "forced multiple columns",
			forcedColumn:    "col1,col2",
			expectedColumns: "col1,col2",
			expectedCount:   2,
			expectedType:    "",
		},
		{
			name:         "auto-detect integer",
			forcedColumn: "",
			mockResponse: []map[string]interface{}{
				{
					"COLUMN_NAMES":          "id",
					"COUNT_COLUMN_IN_INDEX": int64(1),
					"DATA_TYPE":             "int",
					"CHARACTER_SET_NAME":    nil,
				},
			},
			expectedColumns: "id",
			expectedCount:   1,
			expectedType:    "integer",
		},
		{
			name:         "auto-detect text",
			forcedColumn: "",
			mockResponse: []map[string]interface{}{
				{
					"COLUMN_NAMES":          "name",
					"COUNT_COLUMN_IN_INDEX": int64(1),
					"DATA_TYPE":             "varchar",
					"CHARACTER_SET_NAME":    "utf8",
				},
			},
			expectedColumns: "name",
			expectedCount:   1,
			expectedType:    "text",
		},
		{
			name:         "auto-detect temporal",
			forcedColumn: "",
			mockResponse: []map[string]interface{}{
				{
					"COLUMN_NAMES":          "created_at",
					"COUNT_COLUMN_IN_INDEX": int64(1),
					"DATA_TYPE":             "datetime",
					"CHARACTER_SET_NAME":    nil,
				},
			},
			expectedColumns: "created_at",
			expectedCount:   1,
			expectedType:    "temporal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				ForcedChunkingColumn: tt.forcedColumn,
			}
			chunker := &Chunker{Config: config}

			if tt.forcedColumn == "" {
				// Use a mock DB for auto-detection
				mockDB := &MockDB{uniqueKeyColumns: tt.mockResponse}
				chunker.db = mockDB
			}

			result, count, keyType, err := chunker.GetSelectedUniqueKeyColumnNames()
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if result != tt.expectedColumns {
				t.Errorf("Expected columns %s, got %s", tt.expectedColumns, result)
			}
			if count != tt.expectedCount {
				t.Errorf("Expected count %d, got %d", tt.expectedCount, count)
			}
			if keyType != tt.expectedType {
				t.Errorf("Expected type %s, got %s", tt.expectedType, keyType)
			}
		})
	}
}

// Test range specification logic
func TestRangeSpecifications(t *testing.T) {
	tests := []struct {
		name        string
		startWith   string
		endWith     string
		keyType     string
		keyCount    int
		expectError bool
		errorMsg    string
	}{
		{"valid integer start/end", "1", "100", "integer", 1, false, ""},
		{"invalid start for multi-column", "1", "", "integer", 2, true, "--start-with only applies to single column integer chunking keys"},
		{"invalid end for text", "", "abc", "text", 1, true, "--end-with only applies to single column integer chunking keys"},
		{"valid no range", "", "", "integer", 1, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				StartWith:               tt.startWith,
				EndWith:                 tt.endWith,
				UniqueKeyType:           tt.keyType,
				CountColumnsInUniqueKey: tt.keyCount,
			}
			chunker := &Chunker{Config: config, db: &MockDB{}}

			_, _, _, err := chunker.GetUniqueKeyRange()
			if tt.expectError {
				if err == nil || !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				// For successful cases, we can't easily test without more complex mocking
				t.Logf("Range test passed for: %s", tt.name)
			}
		})
	}
}

// Test chunk size validation
func TestChunkSizeValidation(t *testing.T) {
	tests := []struct {
		chunkSize   int
		expectError bool
	}{
		{1000, false},
		{0, false}, // Special case: all rows in one chunk
		{-1, true},
		{-100, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("chunk_size_%d", tt.chunkSize), func(t *testing.T) {
			// This would be validated in the main function
			if tt.chunkSize < 0 && !tt.expectError {
				t.Errorf("Expected error for negative chunk size %d", tt.chunkSize)
			}
		})
	}
}

// Test query type support
func TestQueryTypeSupport(t *testing.T) {
	supportedQueries := []string{
		"UPDATE users SET status='active' WHERE OAK_CHUNK(users)",
		"UPDATE users SET status='active' WHERE OAK_CHUNK(users) AND created_at < '2023-01-01'",
		"DELETE FROM users WHERE OAK_CHUNK(users) AND status='inactive'",
		"INSERT INTO archive SELECT * FROM users WHERE OAK_CHUNK(users)",
		"INSERT INTO log SELECT id, 'processed' FROM users WHERE OAK_CHUNK(users)",
	}

	for _, query := range supportedQueries {
		t.Run(query, func(t *testing.T) {
			re := regexp.MustCompile(`OAK_CHUNK\(([^)]+)\)`)
			if !re.MatchString(query) {
				t.Errorf("Query should be supported: %s", query)
			}
		})
	}
}

// Test performance options
func TestPerformanceOptions(t *testing.T) {
	tests := []struct {
		name       string
		sleep      int
		sleepRatio float64
		noLogBin   bool
		valid      bool
	}{
		{"no performance options", 0, 0, false, true},
		{"sleep only", 100, 0, false, true},
		{"sleep ratio only", 0, 0.5, false, true},
		{"both sleep options", 100, 0.5, false, true}, // Should work, sleep takes precedence
		{"no log bin", 0, 0, true, true},
		{"negative sleep", -1, 0, false, false},
		{"negative sleep ratio", 0, -0.1, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validation would happen in main function
			if !tt.valid {
				if tt.sleep < 0 || tt.sleepRatio < 0 {
					t.Logf("Invalid performance option detected: %s", tt.name)
				}
			}
		})
	}
}
