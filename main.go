package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
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
}
