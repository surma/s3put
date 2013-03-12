`s3put` is a really quick'n'dirty CLI app for s3.

Originally, it has been written during an S3 upload which was taking too long, because there are no tools which support multiple parallel uploads. `s3put` does.
Later, the capability for getting buckets and copying them to the local disk was added.

## Usage

	Usage: s3put [global options] <verb> [verb options]

	Global options:
	        -k, --access-key  AWS Access Key ID (*)
	        -s, --secret-key  AWS Secret Access Key (*)
	        -r, --region      API Region name (default: us-west-1)
	        -b, --bucket      Bucket to push to (*)
	        -c, --concurrency Number of coroutines (default: 10)
	            --continue    Continue on error
	        -h, --help        Show this help

	Verbs:
	    get:
	        -p, --prefix      Only get items starting with prefix
	    put:
	        -p, --prefix      Prefix to prepend to the items

## Binaries

* [Darwin 386](http://filedump.surmair.de/binaries/s3put/darwin_386/s3put)
* [Darwin amd64](http://filedump.surmair.de/binaries/s3put/darwin_amd64/s3put)
* [Freebsd 386](http://filedump.surmair.de/binaries/s3put/freebsd_386/s3put)
* [Freebsd amd64](http://filedump.surmair.de/binaries/s3put/freebsd_amd64/s3put)
* [Linux 386](http://filedump.surmair.de/binaries/s3put/linux_386/s3put)
* [Linux amd64](http://filedump.surmair.de/binaries/s3put/linux_amd64/s3put)
* [Linux arm](http://filedump.surmair.de/binaries/s3put/linux_arm/s3put)
* [Openbsd 386](http://filedump.surmair.de/binaries/s3put/openbsd_386/s3put)
* [Openbsd amd64](http://filedump.surmair.de/binaries/s3put/openbsd_amd64/s3put)
* [Windows 386](http://filedump.surmair.de/binaries/s3put/windows_386/s3put.exe)
* [Windows amd64](http://filedump.surmair.de/binaries/s3put/windows_amd64/s3put.exe)

---
Version 1.1.0
