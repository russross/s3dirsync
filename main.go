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
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"url"
)

const (
	s3_password_file              = "/etc/passwd-amazon-s3"
	s3_access_key_id_variable     = "AWSACCESSKEYID"
	s3_secret_access_key_variable = "AWSSECRETACCESSKEY"
	mime_types_file               = "/etc/mime.types"
	default_cache_location        = "/var/cache/propolis"
	list_request_size             = 256
)

// configuration and state for an active propolis instance
type Propolis struct {
	Bucket            string   // bucket name
	Url               *url.URL // s3 bucket access url
	Secure            bool     // use https
	ReducedRedundancy bool     // use cheaper storage
	Key               string   // Amazon AWS access key
	Secret            string   // Amazon AWS secret key

	BucketRoot string // s3 bucket root directory
	LocalRoot  string // local file system root directory

	Refresh     bool // download list from s3 to refresh cache
	Paranoid    bool // always compute md5 hashes
	Reset       bool // reset the cache before starting
	Directories bool // track directories on s3 with zero-length files
	Practice    bool // do not actually make any changes
	Watch       bool // watch the file system for changes after the initial scan
	Delay       int  // number of seconds to wait before syncing a file
	Concurrent  int  // max number of concurrent server requests

	Db Cache // cache database connection

	Queue      chan *File       // request queue
	Catalog    map[string]*File // file info as found by a refresh scan
	ByContents map[string]*File // md5 hash -> file found by a refresh scan
}

func Setup() (p *Propolis, push bool) {
	var refresh, watch, delete, paranoid, reset, practice, public, secure, reduced, directories bool
	var delay, concurrent int
	flag.BoolVar(&refresh, "refresh", true,
		"Scan online bucket to update cache at startup\n"+
			"\tLonger startup time, but catches changes made while offline")
	flag.BoolVar(&watch, "watch", false,
		"Go into daemon mode and watch the local file system\n"+
			"\tfor changes after initial sync (false means sync then quit)")
	flag.BoolVar(&delete, "delete", true,
		"Delete files when syncing as well as copying changed files")
	flag.BoolVar(&paranoid, "paranoid", false,
		"Always verify md5 hash of file contents,\n"+
			"\teven when all metadata is an exact match (slower)")
	flag.BoolVar(&reset, "reset", false,
		"Reset the cache (implies -refresh=true)")
	flag.BoolVar(&practice, "practice", false,
		"Do a practice run without changing any files\n"+
			"\tShows what would be changed (implies -watch=false)")
	flag.BoolVar(&public, "public", true,
		"Make world-readable local files publicly readable\n"+
			"\tin the online bucket (downloadable via the web)")
	flag.BoolVar(&secure, "secure", false,
		"Use secure connections to Amazon S3\n"+
			"\tA bit slower, but data is encrypted when being transferred")
	flag.BoolVar(&reduced, "reduced", false,
		"Use reduced redundancy storage when uploading\n"+
			"\tCheaper, but higher chance of loosing data")
	flag.BoolVar(&directories, "directories", false,
		"Track directories using special zero-length files\n"+
			"\tMostly useful for greater compatibility with s3fslite")
	flag.IntVar(&delay, "delay", 5,
		"Wait this number of seconds from the last change to a file\n"+
			"\tbefore syncing it with the server")
	flag.IntVar(&concurrent, "concurrent", 25,
		"Maximum number of server transactions that are\n"+
			"\tallowed to run concurrently")

	var accesskeyid, secretaccesskey, cache_location string
	flag.StringVar(&accesskeyid, "accesskeyid", "",
		"Amazon AWS Access Key ID")
	flag.StringVar(&secretaccesskey, "secretaccesskey", "",
		"Amazon AWS Secret Access Key")
	flag.StringVar(&cache_location, "cache", default_cache_location,
		"Metadata cache location\n"+
			"\tA sqlite3 database file that caches online metadata")

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

	// enforce certain option combinations
	if reset {
		refresh = true
	}
	if practice {
		watch = false
	}

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
	var bucketname, bucketprefix, localdir string

	switch {
	case !strings.HasPrefix(args[0], "s3:") && strings.HasPrefix(args[1], "s3:"):
		push = true
		localdir = parseLocalDir(args[0])
		bucketname, bucketprefix = parseBucket(args[1])
	case strings.HasPrefix(args[0], "s3:") && !strings.HasPrefix(args[1], "s3:"):
		push = false
		bucketname, bucketprefix = parseBucket(args[0])
		localdir = parseLocalDir(args[1])
	default:
		flag.Usage()
		os.Exit(-1)
	}

	// make sure the root directory exists
	if info, err := os.Lstat(localdir); err != nil || !info.IsDirectory() {
		fmt.Fprintf(os.Stderr, "%s is not a valid directory\n", localdir)
	}

	// open the database
	var err os.Error
	var cache Cache
	if cache, err = Connect(path.Join(cache_location, bucketname+".sqlite")); err != nil {
		fmt.Println("Error connecting to database:", err)
		os.Exit(-1)
	}

	// create the Propolis object
	url := new(url.URL)
	url.Scheme = "http"
	if secure {
		url.Scheme = "https"
	}
	url.Host = bucketname + ".s3.amazonaws.com"
	url.Path = "/"

	p = &Propolis{
		Bucket:            bucketname,
		Url:               url,
		Secure:            secure,
		ReducedRedundancy: reduced,
		Key:               accesskeyid,
		Secret:            secretaccesskey,

		BucketRoot: bucketprefix,
		LocalRoot:  localdir,

		Refresh:     refresh,
		Paranoid:    paranoid,
		Reset:       reset,
		Directories: directories,
		Practice:    practice,
		Watch:       watch,
		Delay:       delay,
		Concurrent:  concurrent,

		Db: cache,
	}
	return
}

func main() {
	// this exits if there is a problem, so no error checking needed
	p, push := Setup()
	defer p.Db.Close()

	if p.Reset {
		if err := p.ResetCache(); err != nil {
			fmt.Fprintln(os.Stderr, "Error reseting cache:", err)
			os.Exit(-1)
		}
	}

	// scan the server for a catalog of files
	if p.Refresh {
		fmt.Println("Scanning server...")
		catalog, bycontents, err := p.ScanServer(push)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error in refresh scan:", err)
			os.Exit(-1)
		}
		p.Catalog = catalog
		p.ByContents = bycontents
	} else {
		p.Catalog = make(map[string]*File)
	}

	// scan the cache and merge its data with the scanned results
	fmt.Println("Scanning cache...")
	if err := p.ScanCache(push); err != nil {
		fmt.Fprintln(os.Stderr, "Error in cache scan:", err)
		os.Exit(-1)
	}

	// dump cache entries that are out-of-date
	// this removes entries from the catalog as they are processed
	if p.Refresh {
		fmt.Println("Deleting out-of-date cache entries...")
		if err := p.AuditCache(); err != nil {
			fmt.Fprintln(os.Stderr, "Error in cache audit:", err)
			os.Exit(-1)
		}
	}

	q, end := p.StartQueue()
	p.Queue = q

	// do initial file system scan, syncing as we go
	// this removes entries from the catalog as they are processed
	fmt.Println("Scanning file system...")
	if p.Watch {
		panic("Not implemented yet")
	} else {
		scan(p, p.LocalRoot)
	}

	// sync entries found on server but not in local file system
	fmt.Println("Syncing files found on server but not locally...")
	for _, elt := range p.Catalog {
		p.Queue <- elt
	}
	p.Catalog = nil

	fmt.Println("Waiting for queue to empty...")
	done := make(chan bool)
	end <- done
	<-done
	fmt.Println("Finished.")
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
	prefix = path.Clean("/" + prefix)[1:]
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

func (p *Propolis) VisitDir(path string, f *os.FileInfo) bool {
	//q<-FileName{path, true}
	//fmt.Println("Dir :", path)
	p.VisitFile(path+"/", f)
	return true
}

func (p *Propolis) VisitFile(filepath string, f *os.FileInfo) {
	root := p.LocalRoot
	if root != "/" {
		root += "/"
	}
	if !strings.HasPrefix(filepath, root) {
		panic("VisitFile: Invalid prefix [" + filepath + "]")
	}
	name := filepath[len(root):]
	serverpath := path.Join(p.BucketRoot, name)
	var elt *File
	var present bool

	if elt, present = p.Catalog[serverpath]; present {
		// delete it from the catalog once we've processed it
		// note: do this now, now when the file is actually synced
		p.Catalog[serverpath] = nil, false
	} else {
		// TODO: how to know if this is a push?
		push := true
		elt = p.NewFile(name, push, true)
	}

	elt.LocalInfo = f
	p.Queue <- elt
}

func scan(p *Propolis, root string) {
	filepath.Walk(root, p, nil)
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
			chunks := strings.SplitN(s, ":", 2)
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
