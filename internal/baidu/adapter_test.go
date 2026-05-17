package baidu

import "testing"

func TestMapRemoteEntry(t *testing.T) {
	got := MapRemoteEntry(RemoteEntry{
		FSID:        "42",
		ServerName:  "movie.mkv",
		Path:        "/movies/movie.mkv",
		Size:        1024,
		IsDir:       false,
		ServerMTime: 100,
		LocalMTime:  90,
		MD5:         "abc",
	})
	if got.Name != "movie.mkv" || got.Path != "/movies/movie.mkv" || got.Size != 1024 {
		t.Fatalf("unexpected mapped entry: %#v", got)
	}
}

func TestStaticClientListReturnsCopy(t *testing.T) {
	client := &StaticClient{
		Entries: map[string][]RemoteEntry{
			"/movies": {{FSID: "1", ServerName: "a.mkv"}},
		},
	}
	items, err := client.List("/movies")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	items[0].ServerName = "changed"
	again, _ := client.List("/movies")
	if again[0].ServerName != "a.mkv" {
		t.Fatalf("expected copy, got %#v", again[0])
	}
}

func TestMetaToRemoteMapsFSID(t *testing.T) {
	got := metaToRemote(apiFileMeta{
		FSID:       42,
		ServerName: "movie.mkv",
		Path:       "/movies/movie.mkv",
		Size:       1024,
		IsDir:      0,
		MD5:        "abc",
	})
	if got.FSID != "42" || got.ServerName != "movie.mkv" || got.Path != "/movies/movie.mkv" {
		t.Fatalf("unexpected mapped remote entry: %#v", got)
	}
}
