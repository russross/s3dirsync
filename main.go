//
// Propolis
// Main driver
// by Russ Ross <russ@russross.com>
//

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

func main() {
	key := os.Getenv("AWSACCESSKEYID")
	if key == "" {
		fmt.Println("AWSACCESSKEYID undefined")
		os.Exit(-1)
	}
	secret := os.Getenv("AWSSECRETACCESSKEY")
	if secret == "" {
		fmt.Println("AWSSECRETACCESSKEY undefined")
		os.Exit(-1)
	}

	bucket := NewBucket("static.russross.com", "", false, key, secret)

	filename := "s3.go"

	info, _ := os.Lstat(filename)
	err := bucket.CopyRequest(filename, "s3copy.go", info)
	if err != nil {
		fmt.Println("Failed to copy file:", err)
		os.Exit(-1)
	}

	//	buffer := bytes.NewBuffer(nil)
	//	info, err := bucket.DownloadRequest(filename, nopWriteCloser{buffer})
	//	if err != nil {
	//		fmt.Println("Failed to download file:", err)
	//		os.Exit(-1)
	//	}
	//	fmt.Println("Contents:", buffer.String())
	//	fmt.Printf("Metadata:\n%#v\n", *info)

	//	    fp, hash, info, err := bucket.GetFile(filename)
	//		if err != nil {
	//			fmt.Println("Failed to get file:", err)
	//			os.Exit(-1)
	//		}
	//		err = bucket.UploadRequest(filename, fp, hash, info)
	//		if err != nil {
	//			fmt.Println("Failed to execute request:", err)
	//			os.Exit(-1)
	//		}
	//		fmt.Println("request succeeded")
}

// needed just for testing
type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() os.Error { return nil }

// open a file and get info for an upload operation
func (bucket *Bucket) GetFile(path string) (body io.ReadCloser, hash string, info *os.FileInfo, err os.Error) {
	filename := bucket.PathToFileName(path)

	// get file metadata
	info, err = os.Lstat(filename)
	if err != nil {
		return
	}

	// open the file
	if info.Size > 0 {
		fp, err := os.Open(filename)
		if err != nil {
			return
		}
		body = fp

		// compute md5 hash
		md5hash := md5.New()
		if _, err = io.Copy(md5hash, fp); err != nil {
			fp.Close()
			return
		}
		var encoded bytes.Buffer
		encoder := base64.NewEncoder(base64.StdEncoding, &encoded)
		encoder.Write(md5hash.Sum())
		encoder.Close()
		hash = encoded.String()

		// rewind the file
		if _, err = fp.Seek(0, 0); err != nil {
			fp.Close()
			return
		}
	}

	return
}

func UpdateFile(bucket *Bucket, cache *Cache, path string) (err os.Error) {
	// see what is in the local file system
	filename := bucket.PathToFileName(path)
	fsInfo, er := os.Lstat(filename)

	if er != nil {
		// make sure info is nil as a signal for "file not accessible"
		fsInfo = nil
	}

	// see what is on the server
	servername := bucket.PathToServerName(path)
	serverInfo, serverMd5, err := cache.GetFileInfo(servername)
	if err != nil {
		return
	}
	if serverInfo == nil && !bucket.TrustCache {
		if serverInfo, serverMd5, err = bucket.StatRequest(path); err != nil {
			return
		}
	}

	// now compare
	switch {
	case fsInfo == nil && serverInfo == nil:
		// nothing to do
	case fsInfo == nil && serverInfo != nil:
		// delete the file
	case fsInfo != nil && serverInfo == nil ||
		fsInfo.Mode != serverInfo.Mode ||
		fsInfo.Uid != serverInfo.Uid ||
		fsInfo.Gid != serverInfo.Gid ||
		fsInfo.Size != serverInfo.Size ||
		fsInfo.Mtime_ns != serverInfo.Mtime_ns:
		// upload the file

		// get the md5sum of the local file
		// check for a match in the cache
		// copy or upload
	default:
		if !bucket.TrustMetaData {
			// check md5sum for a match
			// upload if different
			fmt.Println(serverMd5)
		}
	}

	return
}
