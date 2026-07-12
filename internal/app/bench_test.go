package app

import (
	"context"
	"os"
	"strings"
	"testing"

	"baidudisklink/internal/auth"
	"baidudisklink/internal/baidu"
)

func TestBenchmarkUsesOfficialDlinkFlow(t *testing.T) {
	a, err := New(Config{
		MountPath:    t.TempDir() + "/mnt",
		TokenPath:    t.TempDir() + "/token.json",
		MetaDBPath:   t.TempDir() + "/meta.db",
		ClientID:     "client",
		ClientSecret: "secret",
		RedirectURI:  "http://127.0.0.1:8765/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auth.SaveToken(auth.Token{AccessToken: "token", RefreshToken: "refresh"}); err != nil {
		t.Fatal(err)
	}
	a.clientFactory = func(token auth.Token) baidu.Client {
		if token.AccessToken != "token" || token.RefreshToken != "refresh" {
			t.Fatalf("unexpected token: %#v", token)
		}
		return &baidu.StaticClient{
			Entries: map[string][]baidu.RemoteEntry{
				"/Videos": {
					{FSID: "1", ServerName: "test.zip", Path: "/Videos/test.zip", Size: 4},
				},
			},
		}
	}
	if err := a.BindRemoteClient(); err != nil {
		t.Fatal(err)
	}
	a.remote.SetClient(&mockBenchClient{
		list: map[string][]baidu.RemoteEntry{
			"/Videos": {
				{FSID: "1", ServerName: "test.zip", Path: "/Videos/test.zip", Size: 2 * 1024 * 1024},
			},
		},
		read: func(fsid string, offset, length int64) ([]byte, error) {
			if fsid != "1" || offset != 0 || length != 8*1024*1024 {
				t.Fatalf("unexpected read args: %s %d %d", fsid, offset, length)
			}
			return []byte(strings.Repeat("a", 8*1024*1024)), nil
		},
	})
	result, err := a.Benchmark("/Videos/test.zip", 4)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != 4 || result.ThroughputMB <= 0 {
		t.Fatalf("unexpected benchmark result: %#v", result)
	}
}

func TestBenchmarkLocalFileReadsRequestedBytes(t *testing.T) {
	path := t.TempDir() + "/test.zip"
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 2*1024*1024)), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := BenchmarkLocalFile(path, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != 1024*1024 {
		t.Fatalf("unexpected local benchmark result: %#v", result)
	}
}

func TestBenchConcurrencyOptionsCanBeApplied(t *testing.T) {
	a, err := New(Config{
		MountPath:           t.TempDir() + "/mnt",
		TokenPath:           t.TempDir() + "/token.json",
		MetaDBPath:          t.TempDir() + "/meta.db",
		ClientID:            "client",
		ClientSecret:        "secret",
		RedirectURI:         "http://127.0.0.1:8765/callback",
		DownloadConcurrency: 4,
		DownloadChunkSize:   4194304,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Remote() == nil {
		t.Fatal("expected remote reader")
	}
}

type mockBenchClient struct {
	list map[string][]baidu.RemoteEntry
	read func(fsid string, offset, length int64) ([]byte, error)
}

func (m *mockBenchClient) List(path string) ([]baidu.RemoteEntry, error) {
	return append([]baidu.RemoteEntry(nil), m.list[path]...), nil
}

func (m *mockBenchClient) Stat(path string) (baidu.RemoteEntry, error) {
	items := m.list[path]
	if len(items) == 0 {
		return baidu.RemoteEntry{}, nil
	}
	return items[0], nil
}

func (m *mockBenchClient) GetDownloadLink(fsid string) (baidu.DownloadLink, error) {
	return baidu.DownloadLink{URL: "https://download.example.invalid/" + fsid}, nil
}

func (m *mockBenchClient) Delete(paths []string) error { return nil }

func (m *mockBenchClient) ReadRange(_ context.Context, fsid string, offset int64, dst []byte) (int, error) {
	if m.read != nil {
		data, err := m.read(fsid, offset, int64(len(dst)))
		if err != nil {
			return 0, err
		}
		return copy(dst, data), nil
	}
	for i := range dst {
		dst[i] = 'x'
	}
	return len(dst), nil
}

func (m *mockBenchClient) RefreshAuth() error { return nil }
