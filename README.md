`s3put` is a really quick'n'dirty CLI app for [S3] and [GCS].

Originally, it has been written during an S3 upload which was taking too long, because there are no tools which support multiple parallel uploads. `s3put` does.
Later, the capability for getting buckets and copying them to the local disk was added.

## Usage

	Usage: s3put [global options] <verb> [verb options]

	Global options:
	        -c, --concurrency   Number of coroutines (default: 10)
	            --continue      Continue on error
	        -p, --prefix        Prefix to apply to remote storage
	            --cache-control Set Cache-Control header on upload
	        -h, --help          Show this help

	Verbs:
	    gcs:
	        -k, --access-key  GCS Interop Access Key ID (*)
	        -s, --secret-key  GCS Interop Access Key (*)
	        -b, --bucket      Bucket URL to push to (*)
	    s3:
	        -k, --access-key  AWS Access Key ID (*)
	        -s, --secret-key  AWS Secret Access Key (*)
	        -b, --bucket      Bucket URL to push to (*)

### Example

	$ s3put -c 5 gcs -k GOOG2MLXXXXXXXXX -s XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -b https://storage.googleapis.com/some-bucket put .
	$ s3put -c 10 s3 -k GOOG2MLXXXXXXXXX -s XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -b https://s3.amazonaws.com/some-bucket get .

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
Version 2.1.0
