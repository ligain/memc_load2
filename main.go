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

func ReadGzFile(done chan struct{}, filename string) (chan []byte, error) {
	rawLines := make(chan []byte)

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Failed to open file: %s", filename)
		return rawLines, err
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		log.Printf("Can not open gzip file: %s", filename)
		return rawLines, err
	}
	defer reader.Close()

	bytesContent, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("Failed to data to buf", err)
		return rawLines, err
	}

	go func() {
		defer close(rawLines)
		for _, byteLine := range bytes.Split(bytesContent, []byte("\n")) {
			select {
			case rawLines <- byteLine:
			case <-done:
				return
			}
		}
	}()
	return rawLines, nil
}

func initMemcConnections(memc_addr map[string]*string, connections map[string]*memcache.Client) {
	for deviceType, deviceHost := range memc_addr {
		connections[deviceType] = memcache.New(*deviceHost)
		connections[deviceType].Timeout = 10 * time.Second
	}
}

func SaveToMemc(preparedApps chan PreparedApps, memcConnections map[string]*memcache.Client, dryRun *bool, processedItems *uint64, errorItems *uint64) {
	for preparedApp := range preparedApps {
		atomic.AddUint64(processedItems, 1)
		client := memcConnections[preparedApp.deviceType]
		if !*dryRun {
			err := client.Set(&memcache.Item{Key: preparedApp.key, Value: preparedApp.value})
			if err != nil {
				atomic.AddUint64(errorItems, 1)
				log.Printf("Error on saving item with key: %s\n", preparedApp.key)
				continue
			}
		}
		log.Printf("Item with key: %s was saved to memcache\n", preparedApp.key)
	}
}

func ParseRawLine(done chan struct{}, rawLines chan []byte) chan PreparedApps {
	var apps []uint32
	var pa PreparedApps
	preparedApps := make(chan PreparedApps)

	go func() {
		defer close(preparedApps)
		for rawLine := range rawLines {
			line := string(rawLine)
			if len(line) == 0 {
				return
			}
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

			select {
			case preparedApps <- pa:
			case <-done:
				return
			}
		}
	}()
	return preparedApps
}

func main() {
	memc_addr := make(map[string]*string, 4)
	dryRun := flag.Bool("dry", false, "Read log files w/o writting to memcache")
	pattern := flag.String("pattern", "/data/appsinstalled/*.tsv.gz", "Path to source data")
	workers := flag.Int("workers", 30, "Workers to load and save data")
	memc_addr["idfa"] = flag.String("idfa", "127.0.0.1:33013", "Memcached host for idfa device type")
	memc_addr["gaid"] = flag.String("gaid", "127.0.0.1:33014", "Memcached host for gaid device type")
	memc_addr["adid"] = flag.String("adid", "127.0.0.1:33015", "Memcached host for adid device type")
	memc_addr["dvid"] = flag.String("dvid", "127.0.0.1:33016", "Memcached host for dvid device type")
	flag.Parse()

	memcConnections := make(map[string]*memcache.Client, 4)
	var wg sync.WaitGroup
	var errorItems uint64 = 0
	var processedItems uint64 = 0

	done := make(chan struct{})
	defer close(done)

	initMemcConnections(memc_addr, memcConnections)

	matches, err := filepath.Glob(*pattern)
	if err != nil {
		log.Fatal(err)
	}

	for _, p := range matches {
		oldPath := path.Clean(p)
		log.Println("Found file: ", oldPath)

		rawLines, _ := ReadGzFile(done, oldPath)
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				parsedLines := ParseRawLine(done, rawLines)
				SaveToMemc(parsedLines, memcConnections, dryRun, &processedItems, &errorItems)
			}()
		}

		dirpath, fpath := path.Split(oldPath)
		newPath := path.Join(dirpath, "."+fpath)
		err := os.Rename(oldPath, newPath)
		if err != nil {
			log.Println("Can not rename file: ", err)
		} else {
			fmt.Printf("Renamed file: %s\n", newPath)
		}
	}
	wg.Wait()

	errorRate := float64(errorItems) / float64(processedItems)
	if errorRate < NormalErrorRate {
		log.Printf("Acceptable error rate (%f). Successful load\n", errorRate)
	} else {
		log.Printf("High error rate (%f > %f). Failed load", errorRate, NormalErrorRate)
	}
	log.Printf("Processed lines: %d", processedItems)
}
