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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
)

type DB struct {
	*sql.DB
}

type Config struct {
	User         string
	Password     string
	Host         string
	Port         int
	Socket       string
	Database     string
	DefaultsFile string
}

func parseMyCnf(configFile string) (map[string]string, error) {
	if configFile == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		configFile = filepath.Join(home, ".my.cnf")
	}

	if strings.HasPrefix(configFile, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		configFile = filepath.Join(home, configFile[2:])
	}

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file %s does not exist", configFile)
	}

	cfg, err := ini.Load(configFile)
	if err != nil {
		return nil, err
	}

	section, err := cfg.GetSection("client")
	if err != nil {
		return nil, err
	}

	config := make(map[string]string)
	if user := section.Key("user").String(); user != "" {
		config["user"] = user
	}
	if password := section.Key("password").String(); password != "" {
		config["password"] = password
	}
	if host := section.Key("host").String(); host != "" {
		config["host"] = host
	}
	if port := section.Key("port").String(); port != "" {
		config["port"] = port
	}
	if socket := section.Key("socket").String(); socket != "" {
		config["socket"] = socket
	}
	if database := section.Key("database").String(); database != "" {
		config["database"] = database
	}

	return config, nil
}

func NewDB(config Config) (*DB, error) {
	var dsn string

	// If defaults file is specified, read from it
	if config.DefaultsFile != "" {
		cnf, err := parseMyCnf(config.DefaultsFile)
		if err != nil {
			return nil, err
		}
		// Override config with values from file
		if user, ok := cnf["user"]; ok && config.User == "" {
			config.User = user
		}
		if password, ok := cnf["password"]; ok && config.Password == "" {
			config.Password = password
		}
		if host, ok := cnf["host"]; ok && config.Host == "" {
			config.Host = host
		}
		if port, ok := cnf["port"]; ok && config.Port == 0 {
			if p, err := strconv.Atoi(port); err == nil {
				config.Port = p
			}
		}
		if socket, ok := cnf["socket"]; ok && config.Socket == "" {
			config.Socket = socket
		}
		if database, ok := cnf["database"]; ok && config.Database == "" {
			config.Database = database
		}
	}

	if config.Host == "localhost" && config.Socket != "" {
		dsn = fmt.Sprintf("%s:%s@unix(%s)/%s?parseTime=true", config.User, config.Password, config.Socket, config.Database)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", config.User, config.Password, config.Host, config.Port, config.Database)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &DB{db}, nil
}

func (db *DB) Exec(query string, args ...interface{}) (int64, error) {
	result, err := db.DB.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (db *DB) QueryRow(query string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, sql.ErrNoRows
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	row := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		row[col] = val
	}

	return row, nil
}

func (db *DB) QueryRows(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			row[col] = val
		}
		results = append(results, row)
	}

	return results, nil
}

func (db *DB) TableExists(database, table string) (bool, error) {
	query := "SELECT COUNT(*) AS count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?"
	row, err := db.QueryRow(query, database, table)
	if err != nil {
		return false, err
	}
	count := row["count"].(int64)
	return count > 0, nil
}

func (db *DB) GetPossibleUniqueKeyColumns(database, table string) ([]map[string]interface{}, error) {
	query := `
		SELECT
		  COLUMNS.TABLE_SCHEMA,
		  COLUMNS.TABLE_NAME,
		  COLUMNS.COLUMN_NAME,
		  UNIQUES.INDEX_NAME,
		  UNIQUES.COLUMN_NAMES,
		  UNIQUES.COUNT_COLUMN_IN_INDEX,
		  COLUMNS.DATA_TYPE,
		  COLUMNS.CHARACTER_SET_NAME
		FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
		  SELECT
			TABLE_SCHEMA,
			TABLE_NAME,
			INDEX_NAME,
			COUNT(*) AS COUNT_COLUMN_IN_INDEX,
			GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
			SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME
		  FROM INFORMATION_SCHEMA.STATISTICS
		  WHERE NON_UNIQUE=0
		  GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
		) AS UNIQUES
		ON (
		  COLUMNS.TABLE_SCHEMA = UNIQUES.TABLE_SCHEMA AND
		  COLUMNS.TABLE_NAME = UNIQUES.TABLE_NAME AND
		  COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
		)
		WHERE
		  COLUMNS.TABLE_SCHEMA = ?
		  AND COLUMNS.TABLE_NAME = ?
		ORDER BY
		  COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
		  CASE UNIQUES.INDEX_NAME
			WHEN 'PRIMARY' THEN 0
			ELSE 1
		  END,
		  CASE IFNULL(CHARACTER_SET_NAME, '')
			  WHEN '' THEN 0
			  ELSE 1
		  END,
		  CASE DATA_TYPE
			WHEN 'tinyint' THEN 0
			WHEN 'smallint' THEN 1
			WHEN 'int' THEN 2
			WHEN 'bigint' THEN 3
			ELSE 100
		  END,
		  COUNT_COLUMN_IN_INDEX
	`
	return db.QueryRows(query, database, table)
}

func (db *DB) LockTableRead(database, table string) error {
	query := fmt.Sprintf("LOCK TABLES `%s`.`%s` READ", database, table)
	_, err := db.Exec(query)
	return err
}

func (db *DB) UnlockTables() error {
	_, err := db.Exec("UNLOCK TABLES")
	return err
}
