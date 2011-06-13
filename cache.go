//
// Propolis
// Cache of file metadata
// by Russ Ross <russ@russross.com>
//

package main

import (
	"gosqlite.googlecode.com/hg/sqlite"
	"os"
)

type Cache struct {
	*sqlite.Conn
}

func Connect(filename string) (db Cache, err os.Error) {
	var c *sqlite.Conn
	if c, err = sqlite.Open(filename); err != nil {
		return
	}
	db = Cache{c}
	err = db.Exec("CREATE TABLE IF NOT EXISTS cache (\n" +
		"    path TEXT NOT NULL,\n" +
		"    md5 TEXT NOT NULL,\n" +
		"    uid INTEGER,\n" +
		"    gid INTEGER,\n" +
		"    mode INTEGER,\n" +
		"    mtime INTEGER,\n" +
		"    size INTEGER,\n" +
		"    flag INTEGER,\n" +
		"    PRIMARY KEY (path)\n" +
		")\n")
	if err != nil {
		db.Close()
		return
	}
	err = db.Exec("CREATE INDEX IF NOT EXISTS idx_md5 ON cache (md5)\n")
	if err != nil {
		db.Close()
		return
	}
	return
}

func (db Cache) GetFileInfo(elt *File) (err os.Error) {
	var stmt *sqlite.Stmt
	stmt, err = db.Prepare("SELECT md5, uid, gid, mode, mtime, size " +
		"FROM cache WHERE path = ?")
	if err != nil {
		return
	}
	defer stmt.Finalize()
	if err = stmt.Exec(elt.ServerPath); err != nil || !stmt.Next() {
		return
	}
	elt.ServerInfo = new(os.FileInfo)
	elt.ServerInfo.Name = elt.ServerPath
	var mode int64
	err = stmt.Scan(
		&elt.ServerHashHex,
		&elt.ServerInfo.Uid,
		&elt.ServerInfo.Gid,
		&mode,
		&elt.ServerInfo.Mtime_ns,
		&elt.ServerInfo.Size)
	elt.ServerInfo.Mode = uint32(mode)
	return
}

func (db Cache) GetPathFromMd5(elt *File) (path string, err os.Error) {
	var stmt1, stmt2 *sqlite.Stmt
	stmt1, err = db.Prepare("SELECT path FROM cache WHERE md5 = ? AND path = ?")
	if err != nil {
		return
	}
	defer stmt1.Finalize()
	if err = stmt1.Exec(elt.LocalHashHex, elt.ServerPath); err != nil {
		return
	}
	if stmt1.Next() {
		// this path has the desired md5 hash
		return elt.ServerPath, nil
	}
	stmt2, err = db.Prepare("SELECT path FROM cache WHERE md5 = ? LIMIT 1")
	if err != nil {
		return
	}
	defer stmt2.Finalize()
	if err = stmt2.Exec(elt.LocalHashHex); err != nil || !stmt2.Next() {
		return
	}
	err = stmt2.Scan(&path)
	return
}

func (db Cache) SetFileInfo(elt *File, uselocal bool) (err os.Error) {
	// clear old entry if it exists
	if err = db.DeleteFileInfo(elt); err != nil {
		return
	}

	// insert new entry
	info := elt.LocalInfo
	hash := elt.LocalHashHex
	if !uselocal {
		info = elt.ServerInfo
		hash = elt.ServerHashHex
	}
	err = db.Exec("INSERT INTO cache VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		info.Name,
		hash,
		info.Uid,
		info.Gid,
		info.Mode,
		info.Mtime_ns,
		info.Size,
		0)
	return
}

func (db Cache) DeleteFileInfo(elt *File) (err os.Error) {
	// delete entry if it exists
	err = db.Exec("DELETE FROM cache WHERE path = ?", elt.ServerPath)
	return
}

func (db Cache) ResetFlags() (err os.Error) {
	err = db.Exec("UPDATE cache SET flag = 0")
	return
}
