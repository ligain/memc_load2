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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type PreparedApps struct {
	deviceType string
	key        string
	value      []byte
}

const NormalErrorRate = 0.01

func ReadGzFile(filename string, c chan<- []byte) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Failed to open file: %s", filename)
		return
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		log.Printf("Can not open gzip file: %s", filename)
		return
	}
	defer reader.Close()

	bytesContent, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("Failed to data to buf", err)
		return
	}

	for _, byteLine := range bytes.Split(bytesContent, []byte("\n")) {
		c <- byteLine
	}
}

func initMemcConnections(memc_addr map[string]*string, connections map[string]*memcache.Client) {
	for deviceType, deviceHost := range memc_addr {
		connections[deviceType] = memcache.New(*deviceHost)
		connections[deviceType].Timeout = 10 * time.Second
	}
}

func SaveToMemc(key string, value []byte, client *memcache.Client, dryRun *bool, errorItems *uint64) {
	if !*dryRun {
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
		log.Println("marshaling error: ", err)
	}
	key := apps_info[0] + apps_info[1]

	pa.deviceType = apps_info[0]
	pa.key = key
	pa.value = encoded_msg
	preparedApps <- pa
}

func main() {
	memc_addr := make(map[string]*string, 4)
	dryRun := flag.Bool("dry", false, "Read log files w/o writting to memcache")
	pattern := flag.String("pattern", "/data/appsinstalled/*.tsv.gz", "Path to source data")
	memc_addr["idfa"] = flag.String("idfa", "127.0.0.1:33013", "Memcached host for idfa device type")
	memc_addr["gaid"] = flag.String("gaid", "127.0.0.1:33014", "Memcached host for gaid device type")
	memc_addr["adid"] = flag.String("adid", "127.0.0.1:33015", "Memcached host for adid device type")
	memc_addr["dvid"] = flag.String("dvid", "127.0.0.1:33016", "Memcached host for dvid device type")
	flag.Parse()

	rawLines := make(chan []byte)
	preparedApps := make(chan PreparedApps)
	memcConnections := make(map[string]*memcache.Client, 4)
	var wg sync.WaitGroup
	var errorItems uint64 = 0
	var processedItems uint64 = 0

	initMemcConnections(memc_addr, memcConnections)

	matches, err := filepath.Glob(*pattern)
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
			dirpath, fpath := path.Split(oldPath)
			newPath := path.Join(dirpath, "."+fpath)
			err := os.Rename(oldPath, newPath)
			if err != nil {
				log.Println("Can not rename file: ", err)
			} else {
				fmt.Printf("Renamed file: %s\n", newPath)
			}
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
	go func() {
		defer wg.Done()
		for pa := range preparedApps {
			atomic.AddUint64(&processedItems, 1)
			SaveToMemc(pa.key, pa.value, memcConnections[pa.deviceType], dryRun, &errorItems)
		}
	}()

	wg.Wait()

	errorRate := float64(errorItems) / float64(processedItems)
	if errorRate < NormalErrorRate {
		log.Printf("Acceptable error rate (%f). Successful load\n", errorRate)
	} else {
		log.Printf("High error rate (%f > %f). Failed load", errorRate, NormalErrorRate)
	}
	log.Printf("Processed lines: %d", processedItems)
}
