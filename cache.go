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

// Cache of file metadata

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

func (p *Propolis) GetFileInfo(elt *File) (err os.Error) {
	var stmt *sqlite.Stmt
	stmt, err = p.Db.Prepare("SELECT md5, uid, gid, mode, mtime, size " +
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

func (p *Propolis) GetPathFromMd5(elt *File) (path string, err os.Error) {
	var stmt1, stmt2 *sqlite.Stmt
	stmt1, err = p.Db.Prepare("SELECT path FROM cache WHERE md5 = ? AND path = ?")
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
	stmt2, err = p.Db.Prepare("SELECT path FROM cache WHERE md5 = ? LIMIT 1")
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

func (p *Propolis) SetFileInfo(elt *File, uselocal bool) (err os.Error) {
	// clear old entry if it exists
	if err = p.DeleteFileInfo(elt); err != nil {
		return
	}

	// insert new entry
	info := elt.LocalInfo
	hash := elt.LocalHashHex
	if !uselocal {
		info = elt.ServerInfo
		hash = elt.ServerHashHex
	}
	err = p.Db.Exec("INSERT INTO cache VALUES (?, ?, ?, ?, ?, ?, ?)",
		info.Name,
		hash,
		info.Uid,
		info.Gid,
		info.Mode,
		info.Mtime_ns,
		info.Size)
	return
}

func (p *Propolis) DeleteFileInfo(elt *File) (err os.Error) {
	// delete entry if it exists
	err = p.Db.Exec("DELETE FROM cache WHERE path = ?", elt.ServerPath)
	return
}

func (p *Propolis) ResetCache() (err os.Error) {
	// clear all cache entries
	err = p.Db.Exec("DELETE FROM cache")
	return
}
