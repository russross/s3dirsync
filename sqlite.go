package main

import (
	"gosqlite.googlecode.com/hg/sqlite"
	"os"
)

type conn struct {
    *sqlite.Conn
}

func Connect(filename string) (db conn, err os.Error) {
    var c *sqlite.Conn
	if c, err = sqlite.Open(filename); err != nil {
		return
	}
    db = conn{c}
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

func (db conn) GetFileInfo(path string) (info *os.FileInfo, md5 string, err os.Error) {
    var stmt *sqlite.Stmt
    stmt, err = db.Prepare("SELECT md5, uid, gid, mode, mtime, size\n" +
            "FROM cache WHERE path = ?")
    if err != nil {
        return
    }
    if err = stmt.Exec(path); err != nil || !stmt.Next() {
        return
    }
    info = new(os.FileInfo)
    info.Name = path
    err = stmt.Scan(&md5, &info.Uid, &info.Gid, &info.Mode, &info.Mtime_ns, &info.Size)
    return
}

func (db conn) SetFileInfo(info *os.FileInfo, md5 string) (err os.Error) {
    // clear old entry if it exists
    if err = db.DeleteFileInfo(info.Name); err != nil {
        return
    }

    // insert new entry
    err = db.Exec("INSERT INTO cache VALUES (?, ?, ?, ?, ?, ?, ?)",
        info.Name, md5,
        info.Uid, info.Gid,
        info.Mode, info.Mtime_ns, info.Size)
    return
}

func (db conn) DeleteFileInfo(path string) (err os.Error) {
    // delete entry if it exists
    err = db.Exec("DELETE FROM cache WHERE path = ?", path)
    return
}
