//
// Propolis
// Amazon S3 transaction handlers
// by Russ Ross <russ@russross.com>
//

package main

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"http"
	"io"
	"net"
	"os"
	"os/user"
	"strings"
	"time"
)

const (
	default_mime_type       = "application/octet-stream"
	directory_mime_type     = "inode/directory"
	symlink_mime_type       = "inode/symlink"
	alt_directory_mime_type = "application/x-directory"
)

const (
	s_ifmt  = 0170000
	s_iflnk = 0120000
	s_ifreg = 0100000
	s_ifdir = 040000

	s_iroth = 04
)

const (
	acl_public  = "public-read"
	acl_private = "private"
)

// in-order list of headers that are included in the request signature
var AWS_HEADERS []string = []string{
	"X-Amz-Acl",
	"X-Amz-Copy-Source",
	"X-Amz-Meta-Gid",
	"X-Amz-Meta-Mode",
	"X-Amz-Meta-Mtime",
	"X-Amz-Meta-Uid",
	"X-Amz-Metadata-Directive",
}

// an S3 bucket
type Bucket struct {
	Bucket     string
	Url        string
	Secure     bool
	UrlPrefix  string
	PathPrefix string
	Key        string
	Secret     string

	TrustCache    bool
	TrustMetaData bool
	MimeTypes     map[string]string
}

func NewBucket(bucket string, urlprefix string, fsprefix string, secure bool, key string, secret string) *Bucket {
	// read in a list of MIME types if possible
	mimes := make(map[string]string)
	if fp, err := os.Open("/etc/mime.types"); err == nil {
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

	url := "http://" + bucket
	if secure {
		url = "https://" + bucket
	}
	url += ".s3.amazonaws.com"
	return &Bucket{
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

func (bucket *Bucket) UploadRequest(elt *File) (err os.Error) {
	_, err = bucket.SendRequest("PUT", "", elt.UrlPath, elt.Contents, elt.LocalHashBase64, elt.LocalInfo)
	return
}


func (bucket *Bucket) DeleteRequest(elt *File) (err os.Error) {
	_, err = bucket.SendRequest("DELETE", "", elt.UrlPath, nil, "", nil)
	return
}

func (bucket *Bucket) StatRequest(elt *File) (err os.Error) {
	var resp *http.Response
	if resp, err = bucket.SendRequest("HEAD", "", elt.UrlPath, nil, "", nil); err != nil {
		// we don't consider "not found" an error
		if resp != nil && resp.StatusCode == 404 {
			err = nil
		}
		return
	}
	elt.ServerInfo = new(os.FileInfo)
	elt.ServerInfo.Name = elt.ServerPath
	bucket.GetResponseMetaData(resp, elt.ServerInfo)
	elt.ServerHashHex = resp.Header.Get("Etag")[1:33]
	return
}

func (bucket *Bucket) CopyRequest(elt *File, src string) (err os.Error) {
	_, err = bucket.SendRequest("PUT", src, elt.UrlPath, nil, "", elt.LocalInfo)
	return
}

func (bucket *Bucket) SetStatRequest(elt *File) (err os.Error) {
	_, err = bucket.SendRequest("PUT", elt.FullServerPath, elt.UrlPath, nil, "", elt.LocalInfo)
	return
}

func (bucket *Bucket) DownloadRequest(path string, body io.WriteCloser) (info *os.FileInfo, err os.Error) {
	var resp *http.Response
	if resp, err = bucket.SendRequest("GET", "", path, nil, "", nil); err != nil {
		return
	}
	info = new(os.FileInfo)
	info.Name = path
	bucket.GetResponseMetaData(resp, info)

	// download and compute MD5 hash as we go
	md5hash := md5.New()

	// adapted from io.Copy
	written := int64(0)
	buf := make([]byte, 32*1024)
	for {
		nr, er := resp.Body.Read(buf)
		if nr > 0 {
			md5hash.Write(buf[0:nr])
			nw, ew := body.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == os.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	body.Close()

	if err == nil && written != info.Size {
		err = io.ErrUnexpectedEOF
	}

	// hex-encode the md5 hash
	md5hex := "\"" + hex.EncodeToString(md5hash.Sum()) + "\""
	if md5hex != resp.Header.Get("Etag") {
		err = os.NewError("md5sum mismatch for " + path)
	}

	return
}

func (bucket *Bucket) SetRequestMetaData(req *http.Request, info *os.FileInfo) {
	// file permissions: grant "public-read" if the file grants world read permission
	if info.Permission()&s_iroth != 0 {
		req.Header.Set("X-Amz-Acl", acl_public)
	} else {
		req.Header.Set("X-Amz-Acl", acl_private)
	}

	// user id: store the numeric and symbolic names
	user, err := user.LookupId(info.Uid)
	if err != nil {
		req.Header.Set("X-Amz-Meta-Uid", fmt.Sprintf("%d", info.Uid))
	} else {
		req.Header.Set("X-Amz-Meta-Uid", fmt.Sprintf("%d (%s)", info.Uid, user.Username))
	}

	// group id: just store the numeric id for now until Go supports looking up group names
	req.Header.Set("X-Amz-Meta-Gid", fmt.Sprintf("%d", info.Gid))

	// store the permissions as an octal number
	req.Header.Set("X-Amz-Meta-Mode", fmt.Sprintf("0%o", info.Mode))

	// store the modified date in a nice format
	sec := info.Mtime_ns / 1e9
	ns := info.Mtime_ns % 1e9
	date := time.SecondsToLocalTime(sec).String()
	if ns == 0 {
		req.Header.Set("X-Amz-Meta-Mtime", fmt.Sprintf("%d (%s)", sec, date))
	} else {
		req.Header.Set("X-Amz-Meta-Mtime", fmt.Sprintf("%d.%09d (%s)", sec, ns, date))
	}

	// set the content-type by looking up the MIME type
	mime := default_mime_type
	switch {
	case info.IsDirectory():
		mime = directory_mime_type
	case info.IsSymlink():
		mime = symlink_mime_type
	default:
		if dot := strings.LastIndex(info.Name, "."); dot >= 0 && dot+1 < len(info.Name) {
			extension := info.Name[dot+1:]
			if kind, present := bucket.MimeTypes[extension]; present {
				mime = kind
			}
		}
	}
	req.Header.Set("Content-Type", mime)
}

func (bucket *Bucket) GetResponseMetaData(resp *http.Response, info *os.FileInfo) {
	// get the user id
	if line := resp.Header.Get("X-Amz-Meta-Uid"); line != "" {
		var uid int
		var username string
		// look up the symbolic name; if found, prefer that; else fall back to numeric id
		switch n, _ := fmt.Sscanf(line, "%d (%s)", &uid, &username); n {
		case 2:
			if localuid, err := user.Lookup(username); err == nil {
				uid = localuid.Uid
			}
		case 1:
		default:
			uid = 0
		}
		info.Uid = uid
	} else {
		info.Uid = 0
	}

	// get the group id
	if line := resp.Header.Get("X-Amz-Meta-Gid"); line != "" {
		var gid int
		if n, _ := fmt.Sscanf(line, "%d", &gid); n != 1 {
			gid = 0
		}
		info.Gid = gid
	} else {
		info.Gid = 0
	}

	// get permissions/file type
	var mode uint32
	if line := resp.Header.Get("X-Amz-Meta-Mode"); line != "" {

		// check for an octal value
		if n, _ := fmt.Sscanf(line, "0%o", &mode); n != 1 {
			// fallback: decimal?
			if n, _ = fmt.Sscanf(line, "%d", &mode); n != 1 {
				mode = 0
			}
		}
	}
	// no mode? try inferring type from Content-Type field
	if mode&s_ifmt == 0 {
		switch {
		case resp.Header.Get("Content-Type") == directory_mime_type:
			mode = 0755 | s_ifdir // permissions + directory
		case resp.Header.Get("Content-Type") == alt_directory_mime_type:
			mode = 0755 | s_ifdir // permissions + directory
		case resp.Header.Get("Content-Type") == symlink_mime_type:
			mode = 0777 | s_iflnk // permissions + symlink
		default:
			mode = 0644 | s_ifreg // permissions + regular file
		}
	}
	info.Mode = mode

	// get the mtime/atime/ctime
	// prefer X-Amz-Meta-Mtime header
	found := false
	var mtime int64
	if line := resp.Header.Get("X-Amz-Meta-Mtime"); line != "" {
		var sec, ns int64
		if n, _ := fmt.Sscanf(line, "%d.%d", &sec, &ns); n == 2 {
			mtime = sec*1e9 + ns
			found = true
		} else {
			if n, _ := fmt.Sscanf(line, "%d", &sec); n == 1 {
				mtime = sec * 1e9
				found = true
			}
		}
	}
	// fall back to Last-Modified
	if !found {
		when, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
		if err != nil {
			mtime = time.Nanoseconds()
		} else {
			mtime = when.Seconds() * 1e9
		}
	}
	info.Atime_ns = mtime
	info.Mtime_ns = mtime
	info.Ctime_ns = mtime

	// get the length from Content-Length
	if line := resp.Header.Get("Content-Length"); line != "" {
		var size int64
		if n, _ := fmt.Sscanf(line, "%d", &size); n == 1 {
			info.Size = size
		} else {
			info.Size = 0
		}
	}
}

func (bucket *Bucket) SendRequest(method string, src, target string, body io.ReadCloser, hash string, info *os.FileInfo) (resp *http.Response, err os.Error) {
	defer func() {
		// if anything goes wrong, close the body reader
		// if it ends normally, this will be closed already and set to nil
		if body != nil {
			body.Close()
		}
	}()

	var req *http.Request
	if req, err = http.NewRequest(method, target, body); err != nil {
		return
	}

	// set upload file info if applicable
	if info != nil && body != nil {
		// TODO: 0-length files fail because the Content-Length field is missing
		// a fix is in the works in the Go library
		req.ContentLength = info.Size
	}

	if info != nil {
		bucket.SetRequestMetaData(req, info)
	}

	// are we uploading a file with a content hash?
	if hash != "" {
		req.Header.Set("Content-MD5", hash)
	}

	// is this a copy/metadata update?
	if src != "" {
		// note: src should already be a full bucket + path name
		req.Header.Set("X-Amz-Copy-Source", urlEncode(src))
		req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
	}

	// sign and execute the request
	// note: 2nd argument is temporary hack to set Content-Length: 0 when needed
	if resp, err = bucket.SignAndExecute(req, method == "PUT" && body == nil || (info != nil && info.Size == 0)); err != nil {
		return
	}

	// body was closed when the request was written out,
	// so nullify the deferred close
	body = nil

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err = os.NewError(resp.Status)
		return
	}

	return
}

// execute a request; date it, sign it, send it
// note: specialcase is temporary hack to set Content-Length: 0 when needed
func (bucket *Bucket) SignAndExecute(req *http.Request, specialcase bool) (resp *http.Response, err os.Error) {
	// time stamp it
	date := time.LocalTime().Format(time.RFC1123)
	req.Header.Set("Date", date)

	// sign the request
	bucket.SignRequest(req)

	// open a connection
	conn, err := net.Dial("tcp", req.URL.Host+":"+req.URL.Scheme)
	if err != nil {
		return nil, err
	}

	// send the request
	if specialcase {
		var buf bytes.Buffer
		req.Write(&buf)
		fixed := bytes.Replace(buf.Bytes(),
			[]byte("User-Agent: Go http package\r\n"),
			[]byte("User-Agent: Go http package\r\nContent-Length: 0\r\n"), 1)
		_, err = conn.Write(fixed)
	} else {
		err = req.Write(conn)
	}
	if err != nil {
		return
	}

	// now read the response
	reader := bufio.NewReader(conn)
	resp, err = http.ReadResponse(reader, req.Method)
	if err != nil {
		return nil, err
	}

	return
}

func (bucket *Bucket) SignRequest(req *http.Request) {
	// gather the string to be signed

	// method
	msg := req.Method + "\n"

	// md5sum
	msg += req.Header.Get("Content-MD5") + "\n"

	// content-type
	msg += req.Header.Get("Content-Type") + "\n"

	// date
	msg += req.Header.Get("Date") + "\n"

	// add headers
	for _, key := range AWS_HEADERS {
		if value := req.Header.Get(key); value != "" {
			msg += strings.ToLower(key) + ":" + value + "\n"
		}
	}

	// resource: the path components should be URL-encoded, but not the slashes
	msg += urlEncode("/" + bucket.Bucket + req.URL.Path)

	// create the signature
	hmac := hmac.NewSHA1([]byte(bucket.Secret))
	hmac.Write([]byte(msg))

	// get a base64 encoding of the signature
	var encoded bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &encoded)
	encoder.Write(hmac.Sum())
	encoder.Close()
	signature := encoded.String()

	req.Header.Set("Authorization", "AWS "+bucket.Key+":"+signature)
}

func urlEncode(path string) string {
	return strings.Replace(http.URLEscape(path), "%2F", "/", -1)
}
