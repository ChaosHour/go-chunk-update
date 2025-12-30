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
	"strconv"
	"strings"
	"time"

	"database/sql"
)

type DBInterface interface {
	Exec(query string, args ...interface{}) (int64, error)
	QueryRow(query string, args ...interface{}) (map[string]interface{}, error)
	TableExists(database, table string) (bool, error)
	GetPossibleUniqueKeyColumns(database, table string) ([]map[string]interface{}, error)
	LockTableRead(database, table string) error
	UnlockTables() error
}

type Config struct {
	Database                 string
	Table                    string
	UniqueKeyColumnNames     string
	CountColumnsInUniqueKey  int
	UniqueKeyType            string
	UniqueKeyColumnNamesList []string
	ChunkSize                int
	StartWith                string
	EndWith                  string
	TerminateOnNotFound      bool
	ForcedChunkingColumn     string
	SkipRetryChunk           bool
	NoLogBin                 bool
	SleepMillis              int
	SleepRatio               float64
	Verbose                  bool
	Debug                    bool
}

type Chunker struct {
	db     DBInterface
	Config Config
}

func NewChunker(db DBInterface, config Config) *Chunker {
	return &Chunker{db: db, Config: config}
}

func (c *Chunker) Verbose(msg string) {
	if c.Config.Verbose {
		fmt.Printf("-- %s\n", msg)
	}
}

func (c *Chunker) formatRangeValue(vals []interface{}) string {
	if len(vals) == 1 {
		return fmt.Sprintf("%v", vals[0])
	}
	strs := make([]string, len(vals))
	for i, v := range vals {
		strs[i] = fmt.Sprintf("%v", v)
	}
	return "(" + strings.Join(strs, ",") + ")"
}

func toFloat(val interface{}) float64 {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0
}

func (c *Chunker) GetSelectedUniqueKeyColumnNames() (string, int, string, error) {
	if c.Config.ForcedChunkingColumn != "" {
		tokens := strings.Split(c.Config.ForcedChunkingColumn, ",")
		if len(tokens) > 1 {
			c.Verbose(fmt.Sprintf("Forced columns: %s", c.Config.ForcedChunkingColumn))
			return c.Config.ForcedChunkingColumn, len(tokens), "", nil
		} else {
			if strings.Contains(c.Config.ForcedChunkingColumn, ":") {
				parts := strings.Split(c.Config.ForcedChunkingColumn, ":")
				c.Verbose(fmt.Sprintf("Forced column %s of type %s", parts[0], parts[1]))
				return parts[0], 1, parts[1], nil
			} else {
				c.Verbose(fmt.Sprintf("Forced column %s of ungiven type", c.Config.ForcedChunkingColumn))
				return c.Config.ForcedChunkingColumn, 1, "", nil
			}
		}
	}

	rows, err := c.db.GetPossibleUniqueKeyColumns(c.Config.Database, c.Config.Table)
	if err != nil {
		return "", 0, "", err
	}
	if len(rows) == 0 {
		return "", 0, "", nil
	}

	row := rows[0]
	columnNames := strings.ToLower(row["COLUMN_NAMES"].(string))
	countColumns := int(row["COUNT_COLUMN_IN_INDEX"].(int64))
	dataType := strings.ToLower(row["DATA_TYPE"].(string))
	charSet := row["CHARACTER_SET_NAME"]

	uniqueKeyType := ""
	if charSet != nil && charSet.(string) != "" {
		uniqueKeyType = "text"
	} else if strings.Contains(dataType, "int") {
		uniqueKeyType = "integer"
	} else if strings.Contains(dataType, "time") || strings.Contains(dataType, "date") {
		uniqueKeyType = "temporal"
	}

	return columnNames, countColumns, uniqueKeyType, nil
}

func (c *Chunker) GetUniqueKeyRange() ([]interface{}, []interface{}, bool, error) {
	minVars := c.getUniqueKeyMinValuesVariables()
	maxVars := c.getUniqueKeyMaxValuesVariables()

	if c.Config.StartWith != "" {
		if c.Config.UniqueKeyType == "integer" && c.Config.CountColumnsInUniqueKey == 1 {
			if startInt, err := strconv.Atoi(c.Config.StartWith); err == nil {
				query := fmt.Sprintf("SELECT %d INTO %s", startInt, minVars)
				_, err := c.db.Exec(query)
				if err != nil {
					return nil, nil, false, err
				}
				c.Verbose(fmt.Sprintf("Starting with: %d", startInt))
			} else {
				row, err := c.db.QueryRow(c.Config.StartWith)
				if err != nil {
					return nil, nil, false, err
				}
				startInt := row["start_with"].(int64)
				query := fmt.Sprintf("SELECT %d INTO %s", startInt, minVars)
				_, err = c.db.Exec(query)
				if err != nil {
					return nil, nil, false, err
				}
				c.Verbose(fmt.Sprintf("Starting with: %d", startInt))
			}
		} else {
			return nil, nil, false, fmt.Errorf("--start-with only applies to single column integer chunking keys")
		}
	} else {
		query := fmt.Sprintf(`
			SELECT %s INTO %s
			FROM %s.%s
			ORDER BY %s LIMIT 1
		`, c.Config.UniqueKeyColumnNames, minVars, c.Config.Database, c.Config.Table, c.Config.UniqueKeyColumnNames)
		_, err := c.db.Exec(query)
		if err != nil {
			return nil, nil, false, err
		}
	}

	if c.Config.EndWith != "" {
		if c.Config.UniqueKeyType == "integer" && c.Config.CountColumnsInUniqueKey == 1 {
			if endInt, err := strconv.Atoi(c.Config.EndWith); err == nil {
				query := fmt.Sprintf("SELECT %d INTO %s", endInt, maxVars)
				_, err := c.db.Exec(query)
				if err != nil {
					return nil, nil, false, err
				}
			} else {
				row, err := c.db.QueryRow(c.Config.EndWith)
				if err != nil {
					return nil, nil, false, err
				}
				endInt := row["end_with"].(int64)
				query := fmt.Sprintf("SELECT %d INTO %s", endInt, maxVars)
				_, err = c.db.Exec(query)
				if err != nil {
					return nil, nil, false, err
				}
			}
		} else {
			return nil, nil, false, fmt.Errorf("--end-with only applies to single column integer chunking keys")
		}
	} else {
		query := fmt.Sprintf(`
			SELECT %s INTO %s
			FROM %s.%s
			ORDER BY %s DESC LIMIT 1
		`, c.Config.UniqueKeyColumnNames, maxVars, c.Config.Database, c.Config.Table, c.Config.UniqueKeyColumnNames)
		_, err := c.db.Exec(query)
		if err != nil {
			return nil, nil, false, err
		}
	}

	query := fmt.Sprintf("SELECT COUNT(*) AS range_exists FROM (SELECT NULL FROM %s.%s LIMIT 1) SEL1", c.Config.Database, c.Config.Table)
	row, err := c.db.QueryRow(query)
	if err != nil {
		return nil, nil, false, err
	}
	val := row["range_exists"]
	rangeExists := val != nil && val.(int64) > 0

	if rangeExists {
		minValues := make([]interface{}, c.Config.CountColumnsInUniqueKey)
		maxValues := make([]interface{}, c.Config.CountColumnsInUniqueKey)
		for i := 0; i < c.Config.CountColumnsInUniqueKey; i++ {
			minVal, err := c.getSessionVariableValue(fmt.Sprintf("unique_key_min_value_%d", i))
			if err != nil {
				return nil, nil, false, err
			}
			minValues[i] = minVal
			maxVal, err := c.getSessionVariableValue(fmt.Sprintf("unique_key_max_value_%d", i))
			if err != nil {
				return nil, nil, false, err
			}
			maxValues[i] = maxVal
		}
		if c.Config.Verbose {
			fmt.Printf("-- %s (min, max) values: (%s, %s)\n", c.Config.UniqueKeyColumnNames, c.formatRangeValue(minValues), c.formatRangeValue(maxValues))
		}
		return minValues, maxValues, true, nil
	}

	return nil, nil, false, nil
}

func (c *Chunker) getUniqueKeyMinValuesVariables() string {
	vars := make([]string, c.Config.CountColumnsInUniqueKey)
	for i := 0; i < c.Config.CountColumnsInUniqueKey; i++ {
		vars[i] = fmt.Sprintf("@unique_key_min_value_%d", i)
	}
	return strings.Join(vars, ",")
}

func (c *Chunker) getUniqueKeyMaxValuesVariables() string {
	vars := make([]string, c.Config.CountColumnsInUniqueKey)
	for i := 0; i < c.Config.CountColumnsInUniqueKey; i++ {
		vars[i] = fmt.Sprintf("@unique_key_max_value_%d", i)
	}
	return strings.Join(vars, ",")
}

func (c *Chunker) getUniqueKeyRangeStartVariables() string {
	vars := make([]string, c.Config.CountColumnsInUniqueKey)
	for i := 0; i < c.Config.CountColumnsInUniqueKey; i++ {
		vars[i] = fmt.Sprintf("@unique_key_range_start_%d", i)
	}
	return strings.Join(vars, ",")
}

func (c *Chunker) getUniqueKeyRangeEndVariables() string {
	vars := make([]string, c.Config.CountColumnsInUniqueKey)
	for i := 0; i < c.Config.CountColumnsInUniqueKey; i++ {
		vars[i] = fmt.Sprintf("@unique_key_range_end_%d", i)
	}
	return strings.Join(vars, ",")
}

func (c *Chunker) getSessionVariableValue(name string) (interface{}, error) {
	query := fmt.Sprintf("SELECT @%s AS %s", name, name)
	row, err := c.db.QueryRow(query)
	if err != nil {
		return nil, err
	}
	return row[name], nil
}

func (c *Chunker) ChunkUpdate(executeQuery string) error {
	if c.Config.NoLogBin {
		_, err := c.db.Exec("SET SESSION SQL_LOG_BIN=0")
		if err != nil {
			return err
		}
	}

	// Get min and max for progress calculation
	minVal, err := c.getSessionVariableValue("unique_key_min_value_0")
	if err != nil {
		return err
	}
	maxVal, err := c.getSessionVariableValue("unique_key_max_value_0")
	if err != nil {
		return err
	}

	// Build queries
	var firstQuery, restQuery string
	if c.Config.CountColumnsInUniqueKey == 1 {
		firstQuery = strings.Replace(executeQuery, "GO_CHUNK("+c.Config.Table+")", fmt.Sprintf("%s >= @unique_key_min_value_0 AND %s < @unique_key_range_end_0", c.Config.UniqueKeyColumnNames, c.Config.UniqueKeyColumnNames), -1)
		restQuery = strings.Replace(executeQuery, "GO_CHUNK("+c.Config.Table+")", fmt.Sprintf("%s > @unique_key_range_start_0 AND %s < @unique_key_range_end_0", c.Config.UniqueKeyColumnNames, c.Config.UniqueKeyColumnNames), -1)
	} else {
		cols := c.Config.UniqueKeyColumnNames
		minVars := c.getUniqueKeyMinValuesVariables()
		startVars := c.getUniqueKeyRangeStartVariables()
		endVars := c.getUniqueKeyRangeEndVariables()
		firstQuery = strings.Replace(executeQuery, "GO_CHUNK("+c.Config.Table+")", fmt.Sprintf("(%s) >= (%s) AND (%s) < (%s)", cols, minVars, cols, endVars), -1)
		restQuery = strings.Replace(executeQuery, "GO_CHUNK("+c.Config.Table+")", fmt.Sprintf("(%s) > (%s) AND (%s) < (%s)", cols, startVars, cols, endVars), -1)
	}

	// Set initial range
	if c.Config.CountColumnsInUniqueKey == 1 {
		query := "SELECT @unique_key_min_value_0 INTO @unique_key_range_start_0"
		_, err = c.db.Exec(query)
		if err != nil {
			return err
		}
	} else {
		minVars := c.getUniqueKeyMinValuesVariables()
		startVars := c.getUniqueKeyRangeStartVariables()
		query := fmt.Sprintf("SELECT %s INTO %s", minVars, startVars)
		_, err = c.db.Exec(query)
		if err != nil {
			return err
		}
	}

	totalAffected := int64(0)
	totalElapsed := time.Duration(0)
	firstRound := true

	for {
		// Set range end
		limit := c.Config.ChunkSize
		if !firstRound {
			limit++
		}
		var whereClause string
		if c.Config.CountColumnsInUniqueKey == 1 {
			whereClause = fmt.Sprintf("%s > @unique_key_range_start_0 AND %s <= @unique_key_max_value_0", c.Config.UniqueKeyColumnNames, c.Config.UniqueKeyColumnNames)
		} else {
			whereClause = fmt.Sprintf("(%s) > (%s) AND (%s) <= (%s)", c.Config.UniqueKeyColumnNames, c.getUniqueKeyRangeStartVariables(), c.Config.UniqueKeyColumnNames, c.getUniqueKeyMaxValuesVariables())
		}
		query := fmt.Sprintf("SELECT %s FROM (SELECT %s FROM %s.%s WHERE %s ORDER BY %s LIMIT %d) t ORDER BY %s DESC LIMIT 1", c.Config.UniqueKeyColumnNames, c.Config.UniqueKeyColumnNames, c.Config.Database, c.Config.Table, whereClause, c.Config.UniqueKeyColumnNames, limit, c.Config.UniqueKeyColumnNames)
		row, err := c.db.QueryRow(query)
		if err != nil {
			if err == sql.ErrNoRows {
				// No more rows, set end to max
				if c.Config.CountColumnsInUniqueKey == 1 {
					_, err = c.db.Exec("SELECT @unique_key_max_value_0 INTO @unique_key_range_end_0")
				} else {
					maxVars := c.getUniqueKeyMaxValuesVariables()
					endVars := c.getUniqueKeyRangeEndVariables()
					_, err = c.db.Exec(fmt.Sprintf("SELECT %s INTO %s", maxVars, endVars))
				}
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			// Normal processing
			if c.Config.CountColumnsInUniqueKey == 1 {
				endVal := row[c.Config.UniqueKeyColumnNames]
				var endValStr string
				if s, ok := endVal.(string); ok {
					endValStr = s
				} else if i, ok := endVal.(int64); ok {
					endValStr = strconv.FormatInt(i, 10)
				} else {
					endValStr = fmt.Sprintf("%v", endVal)
				}
				_, err = c.db.Exec(fmt.Sprintf("SELECT %s INTO @unique_key_range_end_0", endValStr))
			} else {
				vals := make([]string, c.Config.CountColumnsInUniqueKey)
				for i, col := range c.Config.UniqueKeyColumnNamesList {
					val := row[col]
					if s, ok := val.(string); ok {
						vals[i] = fmt.Sprintf("'%s'", s)
					} else {
						vals[i] = fmt.Sprintf("%v", val)
					}
				}
				endVars := c.getUniqueKeyRangeEndVariables()
				_, err = c.db.Exec(fmt.Sprintf("SELECT %s INTO %s", strings.Join(vals, ","), endVars))
			}
			if err != nil {
				return err
			}
		}

		// Get current range for display
		startVal, err := c.getSessionVariableValue("unique_key_range_start_0")
		if err != nil {
			return err
		}
		endVal, err := c.getSessionVariableValue("unique_key_range_end_0")
		if err != nil {
			return err
		}

		// Calculate progress
		progress := 0
		if maxVal != nil && minVal != nil && startVal != nil {
			minF := toFloat(minVal)
			maxF := toFloat(maxVal)
			startF := toFloat(startVal)
			if maxF > minF {
				progress = int((startF - minF) / (maxF - minF) * 100)
			}
		}

		if c.Config.Verbose {
			fmt.Printf("-- Performing chunks range %s, %s, progress: %d%%\n", c.formatRangeValue([]interface{}{startVal}), c.formatRangeValue([]interface{}{endVal}), progress)
		}

		// Check if overflow
		if !firstRound {
			row, err := c.db.QueryRow("SELECT @unique_key_range_start_0 >= @unique_key_max_value_0 AS overflow")
			if err != nil {
				return err
			}
			if row["overflow"].(int64) == 1 {
				break
			}
		}

		q := restQuery
		if firstRound {
			q = firstQuery
		}

		startTime := time.Now()
		affected, err := c.db.Exec(q)
		if err != nil {
			return err
		}
		totalAffected += affected

		elapsed := time.Since(startTime)
		totalElapsed += elapsed
		if c.Config.Verbose {
			fmt.Printf("-- + Rows: %d affected, %d accumulating; seconds: %.1f elapsed; %.1f executed\n", affected, totalAffected, elapsed.Seconds(), totalElapsed.Seconds())
		}

		// Sleep if needed
		if c.Config.SleepMillis > 0 {
			time.Sleep(time.Duration(c.Config.SleepMillis) * time.Millisecond)
		}

		// Update range start
		_, err = c.db.Exec("SELECT @unique_key_range_end_0 INTO @unique_key_range_start_0")
		if err != nil {
			return err
		}

		firstRound = false
	}

	if c.Config.Verbose {
		fmt.Printf("-- Performing chunks range complete. Affected rows: %d\n", totalAffected)
		fmt.Printf("-- Chunk update completed\n")
	}
	return nil
}
