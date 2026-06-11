package baidu

func MapRemoteEntry(r RemoteEntry) Entry {
	return Entry{
		FSID:  r.FSID,
		Path:  r.Path,
		Name:  r.ServerName,
		Size:  r.Size,
		IsDir: r.IsDir,
		MTM:   r.ServerMTime,
		MD5:   r.MD5,
	}
}

type StaticClient struct {
	Entries map[string][]RemoteEntry
}

func (c *StaticClient) List(path string) ([]RemoteEntry, error) {
	return append([]RemoteEntry(nil), c.Entries[path]...), nil
}

func (c *StaticClient) Stat(path string) (RemoteEntry, error) {
	items := c.Entries[path]
	if len(items) == 0 {
		return RemoteEntry{}, nil
	}
	return items[0], nil
}

func (c *StaticClient) GetDownloadLink(fsid string) (DownloadLink, error) {
	return DownloadLink{URL: "https://example.invalid/" + fsid}, nil
}

func (c *StaticClient) Delete(paths []string) error { return nil }

func (c *StaticClient) ReadRange(_ string, _ int64, length int64) ([]byte, error) {
	return make([]byte, length), nil
}

func (c *StaticClient) RefreshAuth() error { return nil }
