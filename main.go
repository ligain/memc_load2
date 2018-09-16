package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"memc_load2/appsinstalled"
	"os"
	"reflect"
	"strconv"
	"strings"
)

var options struct {
	test    bool
	dryRun  bool
	pattern string
	idfa    string
	gaid    string
	adid    string
	dvid    string
}

type AppInfo struct {
	device struct {
		dev_type string
		dev_id   string
	}
	lat  float64
	lon  float64
	apps []uint32
}

func ReadGzFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	result, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return result, nil

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

func main() {
	//path := "/home/linder/PycharmProjects/otus-python/12_concurrency/medium_test_data/20170929000000.tsv.gz"
	flag.BoolVar(&options.test, "test", false, "Test protobuf serialization/deserialization")
	flag.BoolVar(&options.test, "t", false, "Short usage of --test option")
	flag.BoolVar(&options.dryRun, "dry", false, "Read log files w/o writting to memcache")
	flag.StringVar(&options.pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "Path to source data")
	flag.StringVar(&options.idfa, "idfa", "127.0.0.1:33013", "")
	flag.StringVar(&options.gaid, "gaid", "127.0.0.1:33014", "")
	flag.StringVar(&options.adid, "adid", "127.0.0.1:33015", "")
	flag.StringVar(&options.dvid, "dvid", "127.0.0.1:33016", "")
	flag.Parse()

	//gzFile, err := ReadGzFile(path)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println(string(gzFile))

	fmt.Printf("is test: %s\n", options.test)
	fmt.Printf("is dry: %s\n", options.dryRun)
	fmt.Printf("is pattern: %s\n", options.pattern)
	fmt.Printf("is idfa: %s\n", options.idfa)

	if options.test {
		protoTest()
		os.Exit(0)
	}

	//filepath.Glob("sdf")
}
