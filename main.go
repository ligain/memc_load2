package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"memc_load2/appsinstalled"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Options struct {
	test       bool
	dryRun     bool
	pattern    string
	deviceType struct {
		idfa string
		gaid string
		adid string
		dvid string
	}
}

var options Options

type PreparedApps struct {
	deviceType string
	key        string
	value      []byte
}

//const NormalErrorRate = 0.01

func ReadGzFile(filename string, c chan<- []byte) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	bytesContent, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}

	for _, byteLine := range bytes.Split(bytesContent, []byte("\n")) {
		c <- byteLine
	}
}

func initMemcConnections(opt *Options, connections map[string]*memcache.Client) {
	val := reflect.ValueOf(opt.deviceType)
	for i := 0; i < val.NumField(); i++ {
		deviceType := val.Type().Field(i).Name
		deviceHost := val.Field(i).String()
		connections[deviceType] = memcache.New(deviceHost)
	}
}

func SaveToMemc(key string, value []byte, client *memcache.Client, opt *Options, errorItems *uint64, processedItems *uint64) {
	atomic.AddUint64(processedItems, 1)
	if !opt.dryRun {
		if client.Set(&memcache.Item{Key: key, Value: value}) != nil {
			log.Printf("Error on saving item with key: %s\n", key)
			atomic.AddUint64(errorItems, 1)
			return
		}
	}
	log.Printf("Item with key: %s was saved to memcache\n", key)
}

func ParseRawLine(rawLine []byte, preparedApps chan PreparedApps) {
	var apps []uint32
	var pa PreparedApps

	if len(rawLine) == 0 {
		log.Println("Skip an empty line")
		return
	}
	line := string(rawLine)
	fmt.Printf("Parsing line: %s\n", line)
	apps_info := strings.Split(line, "\t")
	raw_apps := strings.Split(apps_info[4], ",")

	for _, app_id := range raw_apps {
		id, err := strconv.ParseInt(app_id, 10, 32)
		if err != nil {
			log.Printf("App id: %s is not an int \n", app_id)
		}
		apps = append(apps, uint32(id))
	}
	lat, err := strconv.ParseFloat(apps_info[2], 32)
	if err != nil {
		log.Println("Can not convert latitude to float")
	}

	lon, err := strconv.ParseFloat(apps_info[3], 32)
	if err != nil {
		log.Println("Can not convert longitute to float")
	}

	msg := &appsinstalled.UserApps{
		Apps: apps,
		Lat:  &lat,
		Lon:  &lon,
	}
	encoded_msg, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	key := apps_info[0] + apps_info[1]

	pa.deviceType = apps_info[0]
	pa.key = key
	pa.value = encoded_msg
	preparedApps <- pa
}

func main() {
	rawLines := make(chan []byte)
	preparedApps := make(chan PreparedApps)
	deviceMap := make(map[string](chan map[string][]byte))
	memcConnections := make(map[string]*memcache.Client, 4)
	var wg sync.WaitGroup

	//var errorItems uint64 = 0
	var processedItems uint64 = 0

	val := reflect.ValueOf(options.deviceType)
	for i := 0; i < val.NumField(); i++ {
		deviceType := val.Type().Field(i).Name
		deviceMap[deviceType] = make(chan map[string][]byte)
	}

	initMemcConnections(&options, memcConnections)

	matches, err := filepath.Glob(options.pattern)
	if err != nil {
		log.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, p := range matches {
			oldPath := path.Clean(p)
			fmt.Println("Found file: ", oldPath)
			ReadGzFile(oldPath, rawLines)
			//dirpath, fpath := path.Split(oldPath)
			//newPath := path.Join(dirpath, "."+fpath)
			//err := os.Rename(oldPath, newPath)
			//if err != nil {
			//	log.Println("Can not rename file: ", err)
			//} else {
			//	fmt.Printf("Renamed file: %s\n", newPath)
			//}
		}
		close(rawLines)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for rawLine := range rawLines {
			ParseRawLine(rawLine, preparedApps)
		}
		close(preparedApps)
	}()

	wg.Add(1)
	go func(processedItems *uint64) {
		defer wg.Done()
		for pa := range preparedApps {
			atomic.AddUint64(processedItems, 1)
			fmt.Printf("%s chan key: %s value: %x\n", pa.deviceType, pa.key, pa.value)
		}
	}(&processedItems)

	wg.Wait()
	log.Printf("Processed lines: %d", processedItems)

	//	errorRate := float64(errorItems) / float64(processedItems)
	//	fmt.Printf("errorRate: %f \n", errorRate)
	//	if errorRate < NormalErrorRate {
	//		log.Printf("Acceptable error rate (%s). Successful load\n", errorRate)
	//	} else {
	//		log.Printf("High error rate (%f > %f). Failed load", errorRate, NormalErrorRate)
	//	}
	//	log.Printf("Processed lines: %d", processedItems)
}

func init() {
	flag.BoolVar(&options.dryRun, "dry", false, "Read log files w/o writting to memcache")
	flag.StringVar(&options.pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "Path to source data")
	flag.StringVar(&options.deviceType.idfa, "idfa", "127.0.0.1:33013", "")
	flag.StringVar(&options.deviceType.gaid, "gaid", "127.0.0.1:33014", "")
	flag.StringVar(&options.deviceType.adid, "adid", "127.0.0.1:33015", "")
	flag.StringVar(&options.deviceType.dvid, "dvid", "127.0.0.1:33016", "")
	flag.Parse()
}
