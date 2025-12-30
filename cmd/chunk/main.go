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
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"go-chunk-update/internal/chunk"
	"go-chunk-update/internal/mysql"
)

var (
	user         string
	host         string
	password     string
	promptPass   bool
	port         int
	socket       string
	defaultsFile string
	database     string
	execute      string
	chunkSize    int
	startWith    string
	endWith      string
	terminateNF  bool
	forceColumn  string
	skipLock     bool
	skipRetry    bool
	noLogBin     bool
	sleepMillis  int
	sleepRatio   float64
	verbose      bool
	debug        bool
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "go-chunk-update",
		Short: "A tool to safely execute large UPDATE/DELETE operations by chunking them.",
		Long:  `A Go port of oak-chunk-update that safely executes large UPDATE/DELETE operations by chunking them.`,
		Run:   runChunkUpdate,
	}

	rootCmd.Flags().StringVarP(&user, "user", "u", "", "MySQL user")
	rootCmd.Flags().StringVarP(&host, "host", "H", "localhost", "MySQL host")
	rootCmd.Flags().StringVarP(&password, "password", "p", "", "MySQL password")
	rootCmd.Flags().BoolVar(&promptPass, "ask-pass", false, "Prompt for password")
	rootCmd.Flags().IntVarP(&port, "port", "P", 3306, "TCP/IP port")
	rootCmd.Flags().StringVarP(&defaultsFile, "defaults-file", "f", "", "Read from MySQL configuration file")
	rootCmd.Flags().StringVarP(&database, "database", "d", "", "Database name")
	rootCmd.Flags().StringVarP(&execute, "execute", "e", "", "Query to execute with GO_CHUNK(table_name)")
	rootCmd.Flags().IntVarP(&chunkSize, "chunk-size", "c", 1000, "Number of rows per chunk")
	rootCmd.Flags().StringVar(&startWith, "start-with", "", "Start chunking from this value")
	rootCmd.Flags().StringVar(&endWith, "end-with", "", "End chunking at this value")
	rootCmd.Flags().BoolVar(&terminateNF, "terminate-on-not-found", false, "Terminate on no rows affected")
	rootCmd.Flags().StringVar(&forceColumn, "force-chunking-column", "", "Force chunking column")
	rootCmd.Flags().BoolVar(&skipLock, "skip-lock-tables", false, "Skip table locking")
	rootCmd.Flags().BoolVar(&skipRetry, "skip-retry-chunk", false, "Skip retry on error")
	rootCmd.Flags().BoolVar(&noLogBin, "no-log-bin", false, "Don't log to binary log")
	rootCmd.Flags().IntVar(&sleepMillis, "sleep", 0, "Sleep between chunks (ms)")
	rootCmd.Flags().Float64Var(&sleepRatio, "sleep-ratio", 0, "Sleep ratio")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	rootCmd.Flags().BoolVar(&debug, "debug", false, "Debug output")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runChunkUpdate(cmd *cobra.Command, args []string) {
	if execute == "" {
		fmt.Println("Error: --execute is required")
		os.Exit(1)
	}

	// Parse query for table
	re := regexp.MustCompile(`GO_CHUNK\(([^)]+)\)`)
	matches := re.FindStringSubmatch(execute)
	if matches == nil {
		fmt.Println("Error: Query must contain GO_CHUNK(table_name)")
		os.Exit(1)
	}
	tableSpec := matches[1]

	dbName := database
	tableTokens := strings.Split(tableSpec, ".")
	tableName := tableTokens[len(tableTokens)-1]
	if len(tableTokens) == 2 {
		dbName = tableTokens[0]
	}

	if dbName == "" {
		fmt.Println("Error: No database specified")
		os.Exit(1)
	}

	// Get password
	pass := password
	if promptPass {
		fmt.Print("Enter password: ")
		bytePass, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatal(err)
		}
		pass = string(bytePass)
		fmt.Println()
	}

	// Connect to DB
	config := mysql.Config{
		User:         user,
		Password:     pass,
		Host:         host,
		Port:         port,
		Socket:       socket,
		Database:     dbName,
		DefaultsFile: defaultsFile,
	}
	db, err := mysql.NewDB(config)
	if err != nil {
		log.Fatal("DB connection error:", err)
	}
	defer db.Close()

	// Check table exists
	exists, err := db.TableExists(dbName, tableName)
	if err != nil {
		log.Fatal("Table check error:", err)
	}
	if !exists {
		log.Fatalf("Table %s.%s does not exist", dbName, tableName)
	}

	// Get unique key
	chunker := chunk.NewChunker(db, chunk.Config{
		Database:             dbName,
		Table:                tableName,
		ChunkSize:            chunkSize,
		StartWith:            startWith,
		EndWith:              endWith,
		TerminateOnNotFound:  terminateNF,
		ForcedChunkingColumn: forceColumn,
		SkipRetryChunk:       skipRetry,
		NoLogBin:             noLogBin,
		SleepMillis:          sleepMillis,
		SleepRatio:           sleepRatio,
		Verbose:              verbose,
		Debug:                debug,
	})

	if verbose {
		fmt.Printf("-- Checking for UNIQUE columns on %s.%s, by which to chunk\n", dbName, tableName)
	}

	uniqueKey, count, keyType, err := chunker.GetSelectedUniqueKeyColumnNames()
	if err != nil {
		log.Fatal("Unique key error:", err)
	}
	if uniqueKey == "" {
		log.Fatal("No unique key found")
	}

	if verbose {
		if forceColumn != "" {
			fmt.Printf("-- Forced column %s of type %s\n", uniqueKey, keyType)
		} else {
			fmt.Printf("-- Found UNIQUE KEY: %s\n", uniqueKey)
		}
	}

	chunker.Config.UniqueKeyColumnNames = uniqueKey
	chunker.Config.CountColumnsInUniqueKey = count
	chunker.Config.UniqueKeyType = keyType
	chunker.Config.UniqueKeyColumnNamesList = strings.Split(uniqueKey, ",")

	// Lock table if needed
	if !skipLock {
		if verbose {
			fmt.Printf("-- Table locked READ\n")
		}
		err = db.LockTableRead(dbName, tableName)
		if err != nil {
			log.Fatal("Lock error:", err)
		}
		defer func() {
			if verbose {
				fmt.Printf("-- Table unlocked\n")
			}
			db.UnlockTables()
		}()
	}

	// Get range
	_, _, rangeExists, err := chunker.GetUniqueKeyRange()
	if err != nil {
		log.Fatal("Range error:", err)
	}
	if !rangeExists {
		fmt.Println("No range to process")
		return
	}

	// Execute chunking
	err = chunker.ChunkUpdate(execute)
	if err != nil {
		log.Fatal("Chunk error:", err)
	}
}
