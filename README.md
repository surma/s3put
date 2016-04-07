`s3put` is a really quick'n'dirty CLI app for [S3] and [GCS].

Originally, it has been written during an S3 upload which was taking too long, because there are no tools which support multiple parallel uploads. `s3put` does.
Later, the capability for getting buckets and copying them to the local disk was added.

## Usage

	Usage: s3put [global options] <get|put> <files...>

	Global options:
			-c, --concurrency   Number of coroutines (default: 10)
				--continue      Continue on error
			-p, --prefix        Prefix to apply to remote storage
				--cache-control Set Cache-Control header on upload
			-k, --access-key    AWS Access Key ID (*)
			-s, --secret-key    AWS Secret Access Key (*)
			-b, --bucket        Bucket URL to push to (*)
			-h, --help          Show this help

### Example

	$ s3put -c 5 -k GOOG2MLXXXXXXXXX -s XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -b gcs://storage.googleapis.com/some-bucket put .
	$ s3put -c 10 -k GOOG2MLXXXXXXXXX -s XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -b s3://s3.amazonaws.com/some-bucket get .

## Binaries

Binaries can be found in the [release section](https://github.com/surma/s3put/releases).

[S3]: https://aws.amazon.com/s3/
[GCS]: https://cloud.google.com/storage/
---
Version 3.0.2
