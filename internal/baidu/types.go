package baidu

import "time"

type RemoteEntry struct {
	FSID        string
	ServerName  string
	Path        string
	Size        int64
	IsDir       bool
	ServerMTime int64
	LocalMTime  int64
	MD5         string
}

type Entry struct {
	FSID  string
	Path  string
	Name  string
	Size  int64
	IsDir bool
	MTM   int64
	MD5   string
}

type DownloadLink struct {
	URL       string
	ExpiresAt time.Time
}

type Client interface {
	List(path string) ([]RemoteEntry, error)
	Stat(path string) (RemoteEntry, error)
	Delete(paths []string) error
	GetDownloadLink(fsid string) (DownloadLink, error)
	ReadRange(fsid string, offset, length int64) ([]byte, error)
	RefreshAuth() error
}
