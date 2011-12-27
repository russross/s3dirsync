//
// Propolis: Amazon S3 <--> local file system synchronizer
// Copyright © 2011 Russ Ross <russ@russross.com>
//
// This file is part of Propolis
//
// Propolis is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// Propolis is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with Propolis.  If not, see <http://www.gnu.org/licenses/>.
//

// Synchronization logic

package main

import (
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
	"url"
)

type File struct {
	LocalPath      string   // full path on the local file system
	ServerPath     string   // full path on the server
	FullServerPath string   // full path on the server including bucket prefix
	Url            *url.URL // url to access this item

	Push      bool // should local state override server state?
	Immediate bool // should changes bypass the normal delay?

	LocalInfo       *os.FileInfo // metadata found locally
	LocalHashHex    string       // md5 hash of local file in hex
	LocalHashBase64 string       // md5 hash of local file in base64
	CacheInfo       *os.FileInfo // metadata found in cache
	CacheHashHex    string       // cached md5 hash of remote file in hex
	ServerHashHex   string       // md5 hash of remote file in hex
	ServerSize      int64        // size as reported by a server scan

	Contents io.ReadCloser
}

const empty_file_md5_hash = "d41d8cd98f00b204e9800998ecf8427e"

func (p *Propolis) NewFile(pathname string, push bool, immediate bool) (elt *File) {
	// form all the different file name variations we need
	elt = new(File)
	elt.LocalPath = filepath.Join(p.LocalRoot, pathname)
	elt.ServerPath = path.Join(p.BucketRoot, pathname)
	elt.FullServerPath = path.Join("/", p.Bucket, elt.ServerPath)
	elt.Url = new(url.URL)
	*elt.Url = *p.Url
	elt.Url.Path = path.Join("/", elt.ServerPath)
	elt.Push = push
	elt.Immediate = immediate
	return
}

func (p *Propolis) NewFileServer(servername string, push bool) (elt *File) {
	root := p.BucketRoot
	if root != "" {
		root += "/"
	}
	if strings.HasPrefix(servername, root) {
		return p.NewFile(servername[len(root):], push, true)
	}
	panic("NewFileServer: path with incorrect prefix [" + servername + "]")
}

// Sync a single file between the local file system and the server.
func (p *Propolis) SyncFile(elt *File) (err os.Error) {
	// see what is in the local file system
	var er os.Error
	if elt.LocalInfo == nil {
		elt.LocalInfo, er = os.Lstat(elt.LocalPath)
		if er != nil {
			elt.LocalInfo = nil
		}
	}
	if elt.LocalInfo != nil {
		elt.LocalInfo.Name = elt.ServerPath
	}

	// see what is on the server
	if err = p.LstatServer(elt); err != nil {
		return
	}

	// decide if anything needs updating
	if elt.LocalInfo == nil && elt.CacheInfo == nil {
		// nothing to do
		fmt.Printf("No such file locally or on server [%s]\n", elt.ServerPath)
		return
	}

	if elt.Push {
		switch {
		case elt.LocalInfo == nil && elt.CacheInfo != nil:
			// delete the remote file
			fmt.Printf("Deleting remote file [%s]\n", elt.ServerPath)
			if p.Practice {
				return
			}

			// delete the file before the metadata: if something goes wrong, the
			// delete request will be repeated on reload, but that's better than
			// leaving a dead file on the server and forgetting about it
			if err = p.DeleteRequest(elt); err != nil {
				return
			}
			// delete the cache entry
			if err = p.DeleteFileInfo(elt); err != nil {
				return
			}

		case (elt.LocalInfo != nil && elt.CacheInfo == nil ||
			elt.LocalInfo.Mode != elt.CacheInfo.Mode ||
			elt.LocalInfo.Uid != elt.CacheInfo.Uid ||
			elt.LocalInfo.Gid != elt.CacheInfo.Gid ||
			elt.LocalInfo.Size != elt.CacheInfo.Size ||
			elt.LocalInfo.Mtime_ns != elt.CacheInfo.Mtime_ns):
			// remote update needed

			err = p.UploadFile(elt)

		case p.Paranoid:
			// compute the local md5 hash
			if err = p.GetMd5(elt); err != nil {
				return
			}

			// do they match?
			if elt.LocalHashHex == elt.CacheHashHex {
				fmt.Printf("No change [%s]\n", elt.ServerPath)
				elt.Contents.Close()
				return
			}

			fmt.Printf("MD5 mismatch, uploading [%s]\n", elt.ServerPath)
			if err = p.UploadFile(elt); err != nil {
				return
			}
		}
	} else {
		// this is a pull request
		switch {
		case elt.LocalInfo != nil && elt.CacheInfo == nil:
			// delete the local file
			fmt.Printf("Deleting local file [%s]\n", elt.ServerPath)
			if p.Practice {
				return
			}

			if err = os.Remove(elt.LocalPath); err != nil {
				return
			}

		case (elt.LocalInfo == nil && elt.CacheInfo != nil ||
			elt.LocalInfo.Mode != elt.CacheInfo.Mode ||
			elt.LocalInfo.Uid != elt.CacheInfo.Uid ||
			elt.LocalInfo.Gid != elt.CacheInfo.Gid ||
			elt.LocalInfo.Size != elt.CacheInfo.Size ||
			elt.LocalInfo.Mtime_ns != elt.CacheInfo.Mtime_ns):
			// local update needed

			err = p.DownloadFile(elt)

		case p.Paranoid:
			// compute the local md5 hash
			if err = p.GetMd5(elt); err != nil {
				return
			}
			elt.Contents.Close()

			// do they match?
			if elt.LocalHashHex == elt.CacheHashHex {
				fmt.Printf("No change [%s]\n", elt.ServerPath)
				return
			}

			// download if different
			fmt.Printf("MD5 mismatch, downloading [%s]\n", elt.ServerPath)
			if err = p.DownloadFile(elt); err != nil {
				return
			}
		}
	}

	return
}

func (p *Propolis) LstatServer(elt *File) (err os.Error) {
	// check the cache (if we don't already have the entry loaded)
	if elt.CacheInfo == nil {
		if err = p.GetFileInfo(elt); err != nil {
			return
		}
	}

	// should we issue a stat request to the server?
	if elt.ServerHashHex != "" && elt.CacheInfo == nil {
		if err = p.StatRequest(elt); err != nil {
			return
		}

		// if we got a hit on the server, update the cache
		if elt.CacheInfo != nil {
			//fmt.Printf("Adding missing cache entry [%s]\n", elt.ServerPath)
			if err = p.SetFileInfo(elt, false); err != nil {
				return
			}
		}
	}
	return
}

// open a file and compute an md5 hash for its contents
// this fills in the hash values and sets the Contents field
// to an open file handle ready to read the file
func (p *Propolis) GetMd5(elt *File) (err os.Error) {
	hash := md5.New()

	switch {
	case elt.LocalInfo.IsSymlink():
		// symlink
		var target string

		// read the link
		if target, err = os.Readlink(elt.LocalPath); err != nil {
			return
		}

		// compute the hash
		hash.Write([]byte(target))

		// wrap it up as an io.ReadCloser
		elt.Contents = ioutil.NopCloser(bytes.NewBufferString(target))

	case elt.LocalInfo.Size == 0 || elt.LocalInfo.IsDirectory():
		// empty file
		var buffer bytes.Buffer
		elt.Contents = ioutil.NopCloser(&buffer)

		// treat directories as empty files
		elt.LocalInfo.Size = 0

	default:
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

func (p *Propolis) UploadFile(elt *File) (err os.Error) {
	// clear cache entry first: if something fails, the update
	// will be repeated on restart
	if elt.CacheInfo != nil {
		if err = p.DeleteFileInfo(elt); err != nil {
			if elt.Contents != nil {
				elt.Contents.Close()
			}
			return
		}
	}

	// is this a kind of file we don't track?
	if elt.ServerPath == "" ||
		(!elt.LocalInfo.IsRegular() &&
			!elt.LocalInfo.IsSymlink() &&
			(!p.Directories || !elt.LocalInfo.IsDirectory())) {
		if elt.Contents != nil {
			elt.Contents.Close()
		}
		if elt.CacheInfo != nil {
			// the current file must have replaced an old regular file
			fmt.Printf("Deleting old file masked by untracked file [%s]\n", elt.ServerPath)
			if p.Practice {
				return
			}

			if err = p.DeleteRequest(elt); err != nil {
				return
			}
			if err = p.DeleteFileInfo(elt); err != nil {
				return
			}
		} else {
			//fmt.Printf("Ignoring untracked file [%s]\n", elt.ServerPath)
		}

		return
	}

	// get the md5sum of the local file
	// note: this treats directories like empty files
	if elt.LocalHashHex == "" {
		if err = p.GetMd5(elt); err != nil {
			return
		}
	}

	// elt.Contents is live now, so make sure it gets closed

	// see if we can do a server-to-server copy
	var src string

	switch {
	case elt.LocalHashHex == empty_file_md5_hash:
		// uploading an empty file is easy; don't bother with anything fancy
		src = ""

	case elt.LocalHashHex == elt.CacheHashHex:
		// this is just a metadata update with no content change
		src = elt.ServerPath

	default:
		// look for another file with the same contents
		// so we can do a server-to-server copy

		// try the scan results first
		if p.Refresh && p.ByContents != nil {
			if entry, present := p.ByContents[elt.LocalHashHex]; present && entry.ServerSize == elt.LocalInfo.Size {
				src = entry.ServerPath
			}
		}

		// try the cache
		if src == "" {
			if src, err = p.GetPathFromMd5(elt); err != nil {
				elt.Contents.Close()
				return
			}
		}
	}

	// we can do a server-to-server copy
	if src != "" {
		fmt.Printf("Copying file [%s] to [%s]\n", src, elt.ServerPath)
		if p.Practice {
			return
		}

		if err = p.CopyRequest(elt, path.Join("/", p.Bucket, src)); err != nil {
			// copy failed, so try a regular upload
			fmt.Printf("Copy failed, uploading [%s]\n", elt.ServerPath)
			if err = p.UploadRequest(elt); err != nil {
				// elt.Contents is closed by upload
				return
			}
		} else {
			elt.Contents.Close()
		}
		if err = p.SetFileInfo(elt, true); err != nil {
			return
		}
		return
	}

	// upload the file
	fmt.Printf("Uploading [%s]\n", elt.ServerPath)
	if p.Practice {
		return
	}

	if err = p.UploadRequest(elt); err != nil {
		// elt.Contents is closed by upload
		return
	}
	if err = p.SetFileInfo(elt, true); err != nil {
		return
	}
	return
}

func (p *Propolis) DownloadFile(elt *File) (err os.Error) {
	// make sure the directory containing this file exists

	// empty files are a special case: no need to download or compute md5

	// try finding another file with the same contents

	// download the file

	// set file metadata

	return
}

func (p *Propolis) ScanServer(push bool) (catalog map[string]*File, bycontents map[string]*File, err os.Error) {
	// scan the entire server directory
	catalog = make(map[string]*File)
	bycontents = make(map[string]*File)

	marker := ""
	truncated := true
	for truncated {
		var listresult *ListBucketResult

		// how long is the prefix that should be chopped off?
		prefixlen := len(p.BucketRoot)

		// if non-empty, it will be followed by an extra slash
		if prefixlen > 0 {
			prefixlen++
		}

		// grab a slice of results
		listresult, err = p.ListRequest(p.BucketRoot, marker, list_request_size, true)
		if err != nil {
			return
		}

		truncated = listresult.IsTruncated
		if len(listresult.Contents) > 0 {
			marker = listresult.Contents[len(listresult.Contents)-1].Key
		}

		// process entries one at a time
		for _, elt := range listresult.Contents {
			// get the entry
			path := elt.Key
			if prefixlen > 0 && !strings.HasPrefix(path, p.BucketRoot+"/") {
				err = os.NewError("Bucket list returned key without required prefix: " + path)
				return
			}
			hash := elt.ETag[1 : len(elt.ETag)-1]
			size := elt.Size

			info := p.NewFileServer(path, push)
			info.ServerHashHex = hash
			info.ServerSize = size
			catalog[path] = info

			// track all non-empty files by content hash
			if hash != empty_file_md5_hash {
				bycontents[hash] = info
			}
		}
	}

	return
}
