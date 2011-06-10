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
	"encoding/hex"
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

	bucket := NewBucket("static.russross.com", "propolis", "", false, key, secret)
	cache, err := Connect("metadata.sqlite")
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		os.Exit(-1)
	}

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file>\n", os.Args[0])
		os.Exit(-1)
	}
	filename := os.Args[1]

	if err = UpdateFile(bucket, cache, filename); err != nil {
		fmt.Fprintln(os.Stderr, "Failed:", err)
		os.Exit(-1)
	}
}

// open a file and get info for an upload operation
func (bucket *Bucket) GetMd5(info *os.FileInfo, path string) (body io.ReadCloser, hashhex string, hash64 string, err os.Error) {
	filename := bucket.PathToFileName(path)

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

		hashhex = hex.EncodeToString(md5hash.Sum())

		var encoded bytes.Buffer
		encoder := base64.NewEncoder(base64.StdEncoding, &encoded)
		encoder.Write(md5hash.Sum())
		encoder.Close()
		hash64 = encoded.String()

		// rewind the file
		if _, err = fp.Seek(0, 0); err != nil {
			fp.Close()
			return
		}
	}

	return
}

func UpdateFile(bucket *Bucket, cache Cache, path string) (err os.Error) {
	// see what is in the local file system
	filename := bucket.PathToFileName(path)
	fsInfo, er := os.Lstat(filename)

	if er != nil {
		// make sure info is nil as a signal for "file not accessible"
		fsInfo = nil
	} else {
		fsInfo.Name = bucket.PathToServerName(path)
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
		if serverInfo != nil {
			// the cache appears to be out of date, so update it
			serverInfo.Name = bucket.PathToServerName(path)

			fmt.Printf("Adding missing cache entry [%s]\n", path)
			if err = cache.SetFileInfo(serverInfo, serverMd5); err != nil {
				return
			}
		}
	}

	// now compare
	switch {
	case fsInfo == nil && serverInfo == nil:
		// nothing to do
		fmt.Printf("No such file locally or on server [%s]\n", path)
	case fsInfo == nil && serverInfo != nil:
		// delete the file
		fmt.Printf("Deleting file [%s]\n", path)
		if err = bucket.DeleteRequest(path); err != nil {
			return
		}
		// delete the cache entry
		if err = cache.DeleteFileInfo(servername); err != nil {
			return
		}
	case fsInfo != nil && serverInfo == nil ||
		fsInfo.Mode != serverInfo.Mode ||
		fsInfo.Uid != serverInfo.Uid ||
		fsInfo.Gid != serverInfo.Gid ||
		fsInfo.Size != serverInfo.Size ||
		fsInfo.Mtime_ns != serverInfo.Mtime_ns:
		// upload the file

		// get the md5sum of the local file
		var body io.ReadCloser
		var md5hex, md5base64 string
		var src string
		if fsInfo.Size > 0 {
			if body, md5hex, md5base64, err = bucket.GetMd5(fsInfo, path); err != nil {
				return
			}

			// check for a match in the cache
			if src, err = cache.GetPathFromMd5(md5hex); err != nil {
				body.Close()
				return
			}
		}

		if src != "" {
			src = "/" + bucket.Bucket + src

			// copy an existing file
			if err = cache.DeleteFileInfo(servername); err != nil {
				body.Close()
				return
			}

			fmt.Printf("Copying file [%s] to [%s]\n", src, servername)
			fmt.Printf("[%s]->[%s]\n", md5hex, serverMd5)
			if err = bucket.CopyRequest(src, path, fsInfo); err != nil {
				// copy failed, so try a regular upload
				fmt.Printf("Copy failed, uploading [%s]\n", path)
				if err = bucket.UploadRequest(path, body, md5base64, fsInfo); err != nil {
					return
				}
			} else {
				body.Close()
			}

			if err = cache.SetFileInfo(fsInfo, md5hex); err != nil {
				return
			}
		} else {
			// upload the file
			fmt.Printf("Uploading [%s]\n", path)
			if err = cache.DeleteFileInfo(servername); err != nil {
				body.Close()
				return
			}
			if err = bucket.UploadRequest(path, body, md5base64, fsInfo); err != nil {
				return
			}
			if err = cache.SetFileInfo(fsInfo, md5hex); err != nil {
				return
			}
		}
	default:
		if !bucket.TrustMetaData && fsInfo.Size > 0 {
			// check md5sum for a match
			var body io.ReadCloser
			var md5hex, md5base64 string
			if body, md5hex, md5base64, err = bucket.GetMd5(fsInfo, path); err != nil {
				return
			}

			// upload if different
			if md5hex != serverMd5 {
				fmt.Printf("MD5 mismatch, uploading [%s]\n", path)
				if err = cache.DeleteFileInfo(servername); err != nil {
					body.Close()
					return
				}
				if err = bucket.UploadRequest(path, body, md5base64, fsInfo); err != nil {
					return
				}
				if err = cache.SetFileInfo(fsInfo, md5hex); err != nil {
					return
				}
			} else {
				fmt.Printf("No change [%s]\n", path)
				body.Close()
			}
		}
	}

	return
}
