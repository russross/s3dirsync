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

// Main startup and configuration code

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	s3_password_file              = "/etc/passwd-amazon-s3"
	s3_access_key_id_variable     = "AWSACCESSKEYID"
	s3_secret_access_key_variable = "AWSSECRETACCESSKEY"
	mime_types_file               = "/etc/mime.types"
)

// configuration and state for an active propolis instance
type Propolis struct {
	Bucket     string
	Url        string
	Secure     bool
	UrlPrefix  string
	PathPrefix string
	Key        string
	Secret     string

	Refresh     bool
	TrustCache  bool
	Paranoid    bool
	Directories bool

	MimeTypes map[string]string

	Db Cache
}

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

func (p *Propolis) NewFile(pathname string) (elt *File) {
	// form all the different file name variations we need
	elt = new(File)
	elt.LocalPath = filepath.Join(p.PathPrefix, pathname)
	elt.ServerPath = path.Join("/", p.UrlPrefix, pathname)
	elt.FullServerPath = path.Join("/", p.Bucket, elt.ServerPath)
	elt.UrlPath = p.Url + elt.ServerPath
	return
}

func ParseOptions() *Propolis {
	var refresh, watch, delete, paranoid, practice, public, secure, directories bool
	flag.BoolVar(&refresh, "refresh", true,
		"Scan online bucket to update cache at startup\n"+
			"\tLonger startup time, but catches changes made while offline")
	flag.BoolVar(&watch, "watch", true,
		"Go into daemon mode and watch the local file system\n"+
			"\tfor changes after initial sync (false means sync then quit)")
	flag.BoolVar(&delete, "delete", true,
		"Delete files when syncing as well as copying changed files")
	flag.BoolVar(&paranoid, "paranoid", false,
		"Always verify md5 hash of file contents,\n"+
			"\teven when all metadata is an exact match (slower)")
	flag.BoolVar(&practice, "practice", false,
		"Do a practice run without changing any files\n"+
			"\tShows what would be changed (implies -watch=false)")
	flag.BoolVar(&public, "public", true,
		"Make world-readable local files publicly readable\n"+
			"\tin the online bucket (downloadable via the web)")
	flag.BoolVar(&secure, "secure", false,
		"Use secure connections to Amazon S3\n"+
			"\tA bit slower, but data is encrypted when being transferred")
	flag.BoolVar(&directories, "directories", false,
		"Track directories using special zero-length files\n"+
			"\tMostly useful for greater compatibility with s3fslite")

	var accesskeyid, secretaccesskey string
	flag.StringVar(&accesskeyid, "accesskeyid", "",
		"Amazon AWS Access Key ID")
	flag.StringVar(&secretaccesskey, "secretaccesskey", "",
		"Amazon AWS Secret Access Key")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"Propolis:\n"+
				"  Amazon S3 <--> local file system synchronizer\n"+
				"  Synchronizes a local directory with an S3 bucket, then\n"+
				"  watches the local directory for changes and automatically\n"+
				"  propogates them to the bucket.\n\n"+
				"  See http://github.com/russross/propolis for details\n\n"+
				"  Copyright 2011 by Russ Ross <russ@russross.com>\n\n"+
				"  Propolis comes with ABSOLUTELY NO WARRANTY.  This is free software, and you\n"+
				"  are welcome to redistribute it under certain conditions.  See the GNU\n"+
				"  General Public Licence for details.\n\n"+
				"Usage:\n"+
				"  To start by syncing remote bucket to match local file system:\n"+
				"      %s [flags] local/dir s3:bucket[:remote/dir]\n"+
				"  To start by syncing local file system to match remote bucket:\n"+
				"      %s [flags] s3:bucket[:remote/dir] local/dir\n\n"+
				"  Amazon Access Key ID and Secret Access Key can be specified in\n"+
				"  one of three ways, listed in decreasing order of precedence.\n"+
				"  Note: both values must be supplied using a single method:\n\n"+
				"      1. On the command line\n"+
				"      2. In the environment variables %s and %s\n"+
				"      3. In the file %s as key:secret on a single line\n\n"+
				"Options:\n",
			os.Args[0], os.Args[0],
			s3_access_key_id_variable, s3_secret_access_key_variable, s3_password_file)
		flag.PrintDefaults()
	}
	flag.Parse()

	// make sure we get access keys
	if accesskeyid == "" || secretaccesskey == "" {
		accesskeyid, secretaccesskey = getKeys()
	}
	if accesskeyid == "" || secretaccesskey == "" {
		fmt.Fprintln(os.Stderr, "Error: Amazon AWS Access Key ID and/or Secret Access Key undefined\n")
		flag.Usage()
		os.Exit(-1)
	}

	// check command-line arguments
	args := flag.Args()
	if len(args) != 2 {
		flag.Usage()
		os.Exit(-1)
	}

	// figure out the direction of sync, parse the bucket and directory info
	var localwins bool
	var bucketname, bucketprefix, localdir string

	switch {
	case !strings.HasPrefix(args[0], "s3:") && strings.HasPrefix(args[1], "s3:"):
		localwins = true
		localdir = parseLocalDir(args[0])
		bucketname, bucketprefix = parseBucket(args[1])
	case strings.HasPrefix(args[0], "s3:") && !strings.HasPrefix(args[1], "s3:"):
		localwins = false
		bucketname, bucketprefix = parseBucket(args[0])
		localdir = parseLocalDir(args[1])
	default:
		flag.Usage()
		os.Exit(-1)
	}

	if info, err := os.Lstat(localdir); err != nil || !info.IsDirectory() {
		fmt.Fprintf(os.Stderr, "%s is not a valid directory\n", localdir)
	}

	fmt.Println("localwins, bucketname, bucketprefix, localdir:", localwins, bucketname, bucketprefix, localdir)

	mimes := ReadMimeTypes()

	var err os.Error
	var cache Cache
	if cache, err = Connect("metadata.sqlite"); err != nil {
		fmt.Println("Error connecting to database:", err)
		os.Exit(-1)
	}

	p := New(bucketname, bucketprefix, localdir, secure, accesskeyid, secretaccesskey, mimes, cache)
	p.Refresh = refresh
	p.TrustCache = true
	p.Paranoid = paranoid
	p.Directories = directories

	return p
}

func ReadMimeTypes() (mimes map[string]string) {
	// read in a list of MIME types if possible
	mimes = make(map[string]string)
	if fp, err := os.Open(mime_types_file); err == nil {
		defer fp.Close()
		read := bufio.NewReader(fp)
		for line, isPrefix, err := read.ReadLine(); err == nil; line, isPrefix, err = read.ReadLine() {
			s := strings.TrimSpace(string(line))
			if isPrefix || len(s) < 3 || s[0] == '#' {
				continue
			}
			s = strings.Replace(s, " ", "\t", -1)
			chunks := strings.Split(s, "\t", -1)
			if len(chunks) < 2 {
				continue
			}
			kind := chunks[0]
			for _, ext := range chunks[1:] {
				if ext != "" {
					mimes[ext] = kind
				}
			}
		}
	}
	return
}

func main() {
	// this exits if there is a problem, so no error checking needed
	p := ParseOptions()
	defer p.Db.Close()

	//	fmt.Println("Issuing a list request")
	//	finished := false
	//	marker := ""
	//    var err os.Error
	//	for !finished {
	//		var list *ListBucketResult
	//		if list, err = p.ListRequest("/", marker, 100, true); err != nil {
	//			fmt.Fprintln(os.Stderr, err)
	//			os.Exit(-1)
	//		}
	//		dumpList(list)
	//		finished = !list.IsTruncated
	//		if len(list.Contents) > 0 {
	//			marker = list.Contents[len(list.Contents)-1].Key
	//		}
	//	}
	//
	_, end := StartQueue(p, 10, 25)
	//
	//	if len(os.Args) != 2 {
	//		fmt.Fprintf(os.Stderr, "Usage: %s <rootdir>\n", os.Args[0])
	//		os.Exit(-1)
	//	}
	//	scan(q, os.Args[1])
	//	//prompt(q)

	fmt.Println("Waiting for queue to empty...")
	done := make(chan bool)
	end <- done
	<-done
	fmt.Println("Quitting")
}

func parseBucket(arg string) (name, prefix string) {
	// sanity check
	if !strings.HasPrefix(arg, "s3:") {
		flag.Usage()
		os.Exit(-1)
	}

	// split it into bucket and name
	name = strings.TrimSpace(arg[len("s3:"):])
	if colon := strings.Index(name, ":"); colon >= 0 {
		prefix = strings.TrimSpace(name[colon+1:])
		name = strings.TrimSpace(name[:colon])
	}

	valid := true
	defer func() {
		if !valid {
			fmt.Fprintln(os.Stdout, "Invalid bucket name")
			flag.Usage()
			os.Exit(-1)
		}
	}()

	// validate and canonicalize bucket part
	// from http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?BucketRestrictions.html
	//     bucket names:
	//     - must be between 3 and 255 characters long
	if len(name) < 3 || len(name) > 255 {
		valid = false
		return
	}

	//     - can contain lowercase letters, numbers, periods, underscores, and dashes
	if strings.IndexFunc(name, func(r int) bool {
		return r != '.' && r != '_' && r != '-' &&
			(r < 'a' || r > 'z') &&
			(r < '0' || r > '9')
	}) >= 0 {
		valid = false
		return
	}

	//     - must start with a number or letter
	if !unicode.IsDigit(int(name[0])) && !unicode.IsLetter(int(name[0])) {
		valid = false
		return
	}

	//     - must not be formatted as an IP address (e.g., 192.168.5.4)
	var a, b, c, d, e int
	if n, _ := fmt.Sscanf(name+"!", "%3d.%3d.%3d.%3d%c", &a, &b, &c, &d, &e); n == 5 {
		if e == '!' &&
			a >= 0 && a <= 255 &&
			b >= 0 && b <= 255 &&
			c >= 0 && c <= 255 &&
			d >= 0 && d <= 255 {
			valid = false
			return
		}
	}

	// validate and canonicalize path part
	prefix = path.Clean("/" + prefix)
	return
}

func parseLocalDir(arg string) string {
	path, err := filepath.Abs(arg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while parsing local path %s: %v\n", arg, err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while parsing local path %s: %v\n", arg, err)
	}
	return path
}

func dumpList(list *ListBucketResult) {
	fmt.Println()
	fmt.Printf("%15s: %v\n", "Name", list.Name)
	fmt.Printf("%15s: %#v\n", "Prefix", list.Prefix)
	fmt.Printf("%15s: %#v\n", "Marker", list.Marker)
	fmt.Printf("%15s: %#v\n", "NextMarker", list.NextMarker)
	fmt.Printf("%15s: %v\n", "MaxKeys", list.MaxKeys)
	fmt.Printf("%15s: %v\n", "IsTruncated", list.IsTruncated)
	for _, elt := range list.Contents {
		fmt.Printf("%-80s %s %d\n", elt.Key, elt.ETag, elt.Size)
	}
}

type Walker chan FileName

func (q Walker) VisitDir(path string, f *os.FileInfo) bool {
	//q<-FileName{path, true}
	//fmt.Println("Dir :", path)
	return true
}

func (q Walker) VisitFile(path string, f *os.FileInfo) {
	fmt.Println("File:", path)
	q <- FileName{path, true}
}

func scan(q chan FileName, root string) {
	filepath.Walk(root, Walker(q), nil)
}

func prompt(q chan FileName) {
	fmt.Println("Type file names to be synced.  A blank line quits")
	for {
		var path string
		if n, err := fmt.Scanln(&path); n != 1 || err != nil {
			break
		}
		q <- FileName{path, false}
	}
}

func getKeys() (key, secret string) {
	key = os.Getenv(s3_access_key_id_variable)
	secret = os.Getenv(s3_secret_access_key_variable)
	if key != "" && secret != "" {
		return
	}

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

	return
}

func New(bucket string, urlprefix string, fsprefix string, secure bool, key string, secret string, mimes map[string]string, cache Cache) *Propolis {
	url := "http://" + bucket
	if secure {
		url = "https://" + bucket
	}
	url += ".s3.amazonaws.com"
	return &Propolis{
		Bucket:     bucket,
		Url:        url,
		Secure:     secure,
		UrlPrefix:  urlprefix,
		PathPrefix: fsprefix,
		Key:        key,
		Secret:     secret,
		MimeTypes:  mimes,
	}
}
