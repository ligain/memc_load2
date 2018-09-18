# MemcLoad v2
A script that concurrently load data to  [memcache](https://memcached.org/)  Data files:

-   [https://cloud.mail.ru/public/2hZL/Ko9s8R9TA](https://cloud.mail.ru/public/2hZL/Ko9s8R9TA)
-   [https://cloud.mail.ru/public/DzSX/oj8RxGX1A](https://cloud.mail.ru/public/DzSX/oj8RxGX1A)
-   [https://cloud.mail.ru/public/LoDo/SfsPEzoGc](https://cloud.mail.ru/public/LoDo/SfsPEzoGc)

### Run
```
$ go version
go version go1.10.2 linux/amd64
$ git https://github.com/ligain/12_concurrency
$ cd 12_concurrency/
$ go get github.com/bradfitz/gomemcache/memcache
$ go run main.go --pattern path/to/datafiles/*.tsv.gz
```
### Tests
```
$ cd 12_concurrency/
$ go run main.go --test
```

### Project Goals
The code is written for educational purposes.