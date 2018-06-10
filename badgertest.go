package main

import (
	"log"
	"time"
	"os"
	"io"
	"fmt"
	"flag"
	"encoding/binary"
	"crypto/sha256"
	"github.com/dgraph-io/badger"
	"github.com/dustin/go-humanize"
)

type CompressMethod int

const (
	CompressGzip = iota
	CompressLz4
)

type LogLevelType int

const (
	LogError = iota
	LogWarn
	LogInfo
)

const SHASize = 32

var pageSize = 16384

var terse bool
var maxPages int64
var logLevel LogLevelType
var inFileName string
var comprMethod CompressMethod
var printHeader bool

var db *badger.DB

//
// Open the badgerdb in /tmp/badger and leave it in global variable db
//
func openDb() {
	var err error

	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
}

//
// If there is an error, print the msg string and then panic.
//
func checkError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s\n", msg)
		panic(err)
	}
}

//
// Echo the input args for debugging
//
func echoArgs() {
	fmt.Printf("Page Size: %d\n", pageSize)
	fmt.Printf("Input file: %s\n", inFileName)
}

//
// Parse the command line arguments
//
// Usage: badgertest -pgsz <pagesize> <inputfile>
//
func parseArgs() {
	var pgSzp = flag.Int("pgsz", 8192, "page size")
	var cMethdp = flag.Int("cmethod", 0, "compression method")
	var terFlagp = flag.Bool("terse", true, "terse output")
	var lgLvlp = flag.Int("loglevel", 0, "log level")
	var pHdrp = flag.Bool("h", false, "print header")

	flag.Parse()

	if len(flag.Args()) == 0 {
		flag.Usage()
		panic("Bad args")
	}

    inFileName = flag.Arg(0)

	if *cMethdp != int(CompressGzip) && *cMethdp != int(CompressLz4) {
		panic("Wrong compression method supplied")
	}

	pageSize = *pgSzp

	comprMethod = CompressMethod(*cMethdp)

	terse = *terFlagp

	logLevel = LogLevelType(*lgLvlp)

	printHeader = *pHdrp
}

//
// Set the key (pageCount uint64) and associate value (256 bit SHA) with it.
//
func setKV(key uint64, val [SHASize] byte) {

	valBuf := val[0:len(val)]

    keyBuf := make([]byte, 8)

	binary.LittleEndian.PutUint64(keyBuf, key)

	err := db.Update(func(txn *badger.Txn) error {
		err := txn.Set(keyBuf, valBuf)
		return err
	})
	checkError(err, "Update error")
}

func main() {
	var pageCount uint64

	openDb()

	parseArgs()

	echoArgs()

	fId1, err := os.Open(inFileName)
	checkError(err, "Open error")

	fInfo, err := fId1.Stat()
	checkError(err, "File Stat")

	fSz := fInfo.Size()

	data := make([]byte, pageSize)

	start := time.Now()

	for {
		count, err := fId1.Read(data)
		if count == 0 || err == io.EOF {
			break
		}
		checkError(err, "Read error")

		sum := sha256.Sum256(data)

		setKV(pageCount, sum)

		pageCount++
	}

	t := time.Now()

	elapsed := t.Sub(start)

	durationSecs := int64(elapsed.Seconds())

	rate := fSz/durationSecs

	fmt.Printf("Computed SHA256 on %v pages FileSz: %s in time %s at rate of %s/sec\n", pageCount, humanize.Bytes(uint64(fSz)), elapsed.String(), humanize.Bytes(uint64(rate)))

	db.Close()
}
