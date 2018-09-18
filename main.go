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
	"runtime"
	"strconv"
	"strings"
	"sync"
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

//var readWg sync.WaitGroup

//type AppInfo struct {
//	device struct {
//		dev_type string
//		dev_id   string
//	}
//	lat  float64
//	lon  float64
//	apps []uint32
//}

func ReadGzFile(filename string, c chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
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

func ReadGzFiles(pattern string, c chan []byte) {
	matches, err := filepath.Glob(pattern)
	wg := &sync.WaitGroup{}
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range matches {
		oldPath := path.Clean(p)
		fmt.Println("Found file: ", oldPath)
		wg.Add(1)
		go ReadGzFile(oldPath, c, wg)
		runtime.Gosched()
		//dirpath, fpath := path.Split(oldPath)
		//newPath := path.Join(dirpath, "."+fpath)
		//err := os.Rename(oldPath, newPath)
		//if err != nil {
		//	log.Println("Can not rename file: ", err)
		//} else {
		//	fmt.Printf("Renamed file: %s\n", newPath)
		//}
	}
	wg.Wait()
	close(c)
}

func ParseAppsInstalled(rawLines chan []byte, deviceMap map[string](chan map[string][]byte), cancelCh chan bool) {
	//defer wg.Done()
	var apps []uint32
	for rawLine := range rawLines {
		if len(rawLine) == 0 {
			continue
		}
		line := string(rawLine)
		apps_info := strings.Split(line, "\t")
		raw_apps := strings.Split(apps_info[4], ",")

		for _, app_id := range raw_apps {
			id, err := strconv.ParseInt(app_id, 10, 32)
			if err != nil {
				log.Printf("App id: %s is not an int \n", app_id)
				continue
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
			continue
		}
		key := apps_info[0] + apps_info[1]
		t := make(map[string][]byte)
		t[key] = encoded_msg
		deviceMap[apps_info[0]] <- t
	}
	cancelCh <- true
}

func initMemcConnections(opt *Options, connections map[string]*memcache.Client) {
	val := reflect.ValueOf(opt.deviceType)
	for i := 0; i < val.NumField(); i++ {
		deviceType := val.Type().Field(i).Name
		deviceHost := val.Field(i).String()
		connections[deviceType] = memcache.New(deviceHost)
	}
}

func SaveToMemc(key string, value []byte, client *memcache.Client, opt *Options)  {
	if !opt.dryRun {
		if client.Set(&memcache.Item{Key: key, Value: value}) != nil {
			log.Printf("Error on saving item with key: %s\n", key)
		}
	}
	log.Printf("Item with key: %s was saved to memcache\n", key)
}

func main() {
	if options.test {
		protoTest()
		os.Exit(0)
	}

	rawLines := make(chan []byte)
	deviceMap := make(map[string](chan map[string][]byte))
	cancelCh := make(chan bool)
	memcConnections := make(map[string]*memcache.Client, 4)

	val := reflect.ValueOf(options.deviceType)
	for i := 0; i < val.NumField(); i++ {
		deviceType := val.Type().Field(i).Name
		deviceMap[deviceType] = make(chan map[string][]byte)
	}

	//readWg.Add(1)
	go ReadGzFiles(options.pattern, rawLines)

	//parseWg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		//parseWg.Add(1)
		go ParseAppsInstalled(rawLines, deviceMap, cancelCh)
		//time.Sleep(time.Microsecond)
	}
	initMemcConnections(&options, memcConnections)

LOOP:
	for {
		select {
		case m := <-deviceMap["idfa"]:
			for k, v := range m {
				go SaveToMemc(k, v, memcConnections["idfa"], &options)
			}
		case m := <-deviceMap["gaid"]:
			for k, v := range m {
				go SaveToMemc(k, v, memcConnections["gaid"], &options)
			}
		case m := <-deviceMap["adid"]:
			for k, v := range m {
				go SaveToMemc(k, v, memcConnections["adid"], &options)
			}
		case m := <-deviceMap["dvid"]:
			for k, v := range m {
				go SaveToMemc(k, v, memcConnections["dvid"], &options)
			}
		case <-cancelCh:
			break LOOP
		}
	}
	fmt.Printf("deviceMap:: %#v\n", deviceMap)

	fmt.Printf("is test: %s\n", options.test)
	fmt.Printf("is dry: %s\n", options.dryRun)
	fmt.Printf("is pattern: %s\n", options.pattern)
	fmt.Printf("is idfa: %s\n", options.deviceType.idfa)

	for key, val := range memcConnections {
		fmt.Printf("conn key: %s conn pointer: %p\n", key, val)
	}

	v := reflect.ValueOf(options.deviceType)

	for i := 0; i < v.NumField(); i++ {
		fmt.Printf("name: %s, value: %s \n",
			v.Type().Field(i).Name,
			v.Field(i),
		)
	}

}

func protoTest() {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	lines := strings.Split(sample, "\n")
	for _, line := range lines {
		var apps []uint32

		apps_info := strings.Split(line, "\t")
		raw_apps := strings.Split(apps_info[4], ",")

		for _, app_id := range raw_apps {
			id, err := strconv.ParseInt(app_id, 10, 32)
			if err != nil {
				log.Printf("App id: %s is not an int \n", app_id)
				continue
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
			return
		}

		decoded_msg := new(appsinstalled.UserApps)
		err = proto.Unmarshal(encoded_msg, decoded_msg)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
			return
		}

		if reflect.DeepEqual(msg.Apps, decoded_msg.Apps) {
			fmt.Println("Serialization is successful!")
		}
	}
}

func init() {
	flag.BoolVar(&options.test, "test", false, "Test protobuf serialization/deserialization")
	flag.BoolVar(&options.test, "t", false, "Short usage of --test option")
	flag.BoolVar(&options.dryRun, "dry", false, "Read log files w/o writting to memcache")
	flag.StringVar(&options.pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "Path to source data")
	flag.StringVar(&options.deviceType.idfa, "idfa", "127.0.0.1:33013", "")
	flag.StringVar(&options.deviceType.gaid, "gaid", "127.0.0.1:33014", "")
	flag.StringVar(&options.deviceType.adid, "adid", "127.0.0.1:33015", "")
	flag.StringVar(&options.deviceType.dvid, "dvid", "127.0.0.1:33016", "")
	flag.Parse()
}
