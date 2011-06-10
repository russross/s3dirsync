//
// Propolis
// Main driver
// by Russ Ross <russ@russross.com>
//

package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const s3_password_file = "/etc/passwd-s3fs"

type File struct {
	LocalPath      string
	ServerPath     string
	FullServerPath string
	UrlPath        string

	LocalInfo  *os.FileInfo
	ServerInfo *os.FileInfo

	LocalHashHex    string
	LocalHashBase64 string
	ServerHashHex   string

	Contents io.ReadCloser
}

func (bucket *Bucket) NewFile(pathname string) (elt *File) {
	// form all the different file name variations we need
	elt = new(File)
	elt.LocalPath = filepath.Join(bucket.PathPrefix, pathname)
	elt.ServerPath = path.Join("/", bucket.UrlPrefix, pathname)
	elt.FullServerPath = path.Join("/", bucket.Bucket, elt.ServerPath)
	elt.UrlPath = bucket.Url + elt.ServerPath
	return
}

func main() {
	key := os.Getenv("AWSACCESSKEYID")
	secret := os.Getenv("AWSSECRETACCESSKEY")
	if key == "" || secret == "" {
		// try reading from password file
		fp, err := os.Open(s3_password_file)
		if err == nil {
			read := bufio.NewReader(fp)
			for line, isPrefix, err := read.ReadLine(); err == nil; line, isPrefix, err = read.ReadLine() {
				s := strings.TrimSpace(string(line))
				if isPrefix || len(s) == 0 || s[0] == '#' {
					continue
				}
				chunks := strings.Split(s, ":", 2)
				if len(chunks) != 2 {
					continue
				}
				key = chunks[0]
				secret = chunks[1]
				break
			}
			fp.Close()
		}
	}
	if key == "" {
		fmt.Println("AWSACCESSKEYID undefined")
		os.Exit(-1)
	}
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
	defer cache.Close()

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file>\n", os.Args[0])
		os.Exit(-1)
	}

	file := bucket.NewFile(os.Args[1])

	if err = UpdateFile(bucket, cache, file); err != nil {
		fmt.Fprintln(os.Stderr, "Failed:", err)
		os.Exit(-1)
	}
}

// open a file and compute an md5 hash for its contents
// this fills in the hash values and sets the Contents field
// to an open file handle ready to read the file
// if file has Size == 0, this function does nothing
func (bucket *Bucket) GetMd5(elt *File) (err os.Error) {
	// don't bother for empty files
	if elt.LocalInfo.Size == 0 || elt.LocalInfo.IsDirectory() {
		return
	}

	hash := md5.New()

	// is it a symlink?
	if elt.LocalInfo.IsSymlink() {
		// read the link
		var target string
		if target, err = os.Readlink(elt.LocalPath); err != nil {
			return
		}

		// compute the hash
		hash.Write([]byte(target))

		// wrap it up as an io.ReadCloser
		elt.Contents = ioutil.NopCloser(bytes.NewBufferString(target))
	} else {
		// regular file
		var fp *os.File
		if fp, err = os.Open(elt.LocalPath); err != nil {
			return
		}

		// compute md5 hash
		if _, err = io.Copy(hash, fp); err != nil {
			fp.Close()
			return
		}
		// rewind the file
		if _, err = fp.Seek(0, 0); err != nil {
			fp.Close()
			return
		}
		elt.Contents = fp
	}

	// get the hash in hex
	sum := hash.Sum()
	elt.LocalHashHex = hex.EncodeToString(sum)

	// and in base64
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	encoder.Write(sum)
	encoder.Close()
	elt.LocalHashBase64 = buf.String()

	return
}

func UpdateFile(bucket *Bucket, cache Cache, elt *File) (err os.Error) {
	// see what is in the local file system
	var er os.Error
	elt.LocalInfo, er = os.Lstat(elt.LocalPath)

	if er != nil {
		// make sure info is nil as a signal that the file doesn't exist or is not accessible
		elt.LocalInfo = nil
	} else {
		elt.LocalInfo.Name = elt.ServerPath
	}

	// see what is on the server
	if err = cache.GetFileInfo(elt); err != nil {
		return
	}
	switch {
	case elt.ServerInfo == nil && !bucket.TrustCacheIsComplete:
		if err = bucket.StatRequest(elt); err != nil {
			return
		}
		if elt.ServerInfo != nil && elt.ServerHashHex != "" {
			// the cache appears to be out of date, so update it
			fmt.Printf("Adding missing cache entry [%s]\n", elt.ServerPath)
			if err = cache.SetFileInfo(elt, false); err != nil {
				return
			}
		}

	case elt.ServerInfo != nil && !bucket.TrustCacheIsAccurate:
		cacheinfo := elt.ServerInfo
		cachehash := elt.ServerHashHex
		elt.ServerInfo = nil
		elt.ServerHashHex = ""
		if err = bucket.StatRequest(elt); err != nil {
			return
		}
		if elt.ServerInfo == nil || elt.ServerHashHex == "" {
			// cache said we had something, server disagrees
			fmt.Printf("Removing bogus cache entry [%s]\n", elt.ServerPath)
			if err = cache.DeleteFileInfo(elt); err != nil {
				return
			}
		} else {
			// see if the server and the cache disagree
			if cachehash != elt.ServerHashHex ||
				cacheinfo.Uid != elt.ServerInfo.Uid ||
				cacheinfo.Gid != elt.ServerInfo.Gid ||
				cacheinfo.Mode != elt.ServerInfo.Mode ||
				cacheinfo.Mtime_ns != elt.ServerInfo.Mtime_ns ||
				cacheinfo.Size != elt.ServerInfo.Size {

				fmt.Printf("Updating bogus cache entry [%s]\n", elt.ServerPath)
				if err = cache.SetFileInfo(elt, false); err != nil {
					return
				}
			}
		}
	}

	// now compare
	switch {
	case elt.LocalInfo == nil && elt.ServerInfo == nil:
		// nothing to do
		fmt.Printf("No such file locally or on server [%s]\n", elt.ServerPath)
		return

	case elt.LocalInfo == nil && elt.ServerInfo != nil:
		// delete the file
		fmt.Printf("Deleting file [%s]\n", elt.ServerPath)

		// delete the file before the metadata: if something goes wrong, the
		// delete request will be repeated on reload, but that's better than
		// leaving a dead file on the server and forgetting about it
		if err = bucket.DeleteRequest(elt); err != nil {
			return
		}
		// delete the cache entry
		if err = cache.DeleteFileInfo(elt); err != nil {
			return
		}
		return

	case elt.LocalInfo != nil && elt.ServerInfo == nil ||
		elt.LocalInfo.Mode != elt.ServerInfo.Mode ||
		elt.LocalInfo.Uid != elt.ServerInfo.Uid ||
		elt.LocalInfo.Gid != elt.ServerInfo.Gid ||
		elt.LocalInfo.Size != elt.ServerInfo.Size ||
		elt.LocalInfo.Mtime_ns != elt.ServerInfo.Mtime_ns:
		// server needs an update

		// clear cache entry first: if something fails, the update
		// will be repeated on restart
		if elt.ServerInfo != nil {
			if err = cache.DeleteFileInfo(elt); err != nil {
				return
			}
		}

		// is this a kind of file we don't track?
		if !elt.LocalInfo.IsRegular() &&
			!elt.LocalInfo.IsSymlink() &&
			(!bucket.TrackDirectories || !elt.LocalInfo.IsDirectory()) {
			if elt.ServerInfo != nil {
				// the current file must have replaced an old regular file
				fmt.Printf("Deleting old file masked by untracked file [%s]\n", elt.ServerPath)
				if err = bucket.DeleteRequest(elt); err != nil {
					return
				}
				if err = cache.DeleteFileInfo(elt); err != nil {
					return
				}
			} else {
				fmt.Printf("Ignoring untracked file [%s]\n", elt.ServerPath)
			}

			return
		}

		// is it an empty file?
		if elt.LocalInfo.Size == 0 || elt.LocalInfo.IsDirectory() {
			fmt.Printf("Uploading zero-length file [%s]\n", elt.ServerPath)
			if err = bucket.UploadRequest(elt); err != nil {
				return
			}
			if err = cache.SetFileInfo(elt, true); err != nil {
				return
			}
			return
		}

		// get the md5sum of the local file
		if err = bucket.GetMd5(elt); err != nil {
			return
		}

		// elt.Contents is live now, so make sure it gets closed

		var src string
		if elt.LocalHashHex == elt.ServerHashHex {
			// this is just a metadata update with no content change
			src = elt.ServerPath
		} else {
			// look for another file with the same contents
			// so we can do a server-to-server copy
			if src, err = cache.GetPathFromMd5(elt); err != nil {
				elt.Contents.Close()
				return
			}
		}

		if src == "" {
			// upload the file
			fmt.Printf("Uploading [%s]\n", elt.ServerPath)
			if err = bucket.UploadRequest(elt); err != nil {
				// elt.Contents is closed by upload
				return
			}
			if err = cache.SetFileInfo(elt, true); err != nil {
				return
			}
			return
		}

		// copy an existing file
		fmt.Printf("Copying file [%s] to [%s]\n", src, elt.ServerPath)
		if err = bucket.CopyRequest(elt, "/"+bucket.Bucket+src); err != nil {
			// copy failed, so try a regular upload
			fmt.Printf("Copy failed, uploading [%s]\n", elt.ServerPath)
			if err = bucket.UploadRequest(elt); err != nil {
				// elt.Contents is closed by upload
				return
			}
		} else {
			elt.Contents.Close()
		}
		if err = cache.SetFileInfo(elt, true); err != nil {
			return
		}

		return

	default:
		if !bucket.AlwaysHashContents && elt.LocalInfo.Size > 0 {
			// check md5sum for a match
			if err = bucket.GetMd5(elt); err != nil {
				return
			}

			// upload if different
			if elt.LocalHashHex != elt.ServerHashHex {
				fmt.Printf("MD5 mismatch, uploading [%s]\n", elt.ServerPath)
				if err = bucket.UploadRequest(elt); err != nil {
					return
				}
				if err = cache.SetFileInfo(elt, true); err != nil {
					return
				}
			} else {
				fmt.Printf("No change [%s]\n", elt.ServerPath)
				elt.Contents.Close()
			}
		}
	}

	return
}
