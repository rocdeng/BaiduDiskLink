package remote

import (
	"errors"
	"testing"
	"time"

	"baidudisklink/internal/baidu"
)

type stubClient struct {
	downloadCalls int
	readCalls     int
	failRead      bool
}

func (s *stubClient) List(path string) ([]baidu.RemoteEntry, error) { return nil, nil }
func (s *stubClient) Stat(path string) (baidu.RemoteEntry, error)   { return baidu.RemoteEntry{}, nil }
func (s *stubClient) GetDownloadLink(fsid string) (baidu.DownloadLink, error) {
	s.downloadCalls++
	return baidu.DownloadLink{URL: "https://example.invalid/" + fsid, ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (s *stubClient) ReadRange(fsid string, offset, length int64) ([]byte, error) {
	s.readCalls++
	if s.failRead {
		return nil, errors.New("transient read failure")
	}
	return make([]byte, length), nil
}
func (s *stubClient) RefreshAuth() error { return nil }

func TestReadRangeUsesRequestedLength(t *testing.T) {
	r := NewReader(nil)
	got, err := r.ReadRange("fsid-1", 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 bytes, got %d", len(got))
	}
}

func TestReadRangeRejectsNegativeLength(t *testing.T) {
	r := NewReader(nil)
	_, err := r.ReadRange("fsid-1", 10, -1)
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestReadRangeCachesDownloadLinkAndRetries(t *testing.T) {
	client := &stubClient{failRead: true}
	r := NewReader(client)
	_, err := r.ReadRange("fsid-1", 0, 8)
	if err == nil {
		t.Fatal("expected error from stub client")
	}
	if client.downloadCalls != 1 {
		t.Fatalf("expected one download link request, got %d", client.downloadCalls)
	}
	if client.readCalls != 2 {
		t.Fatalf("expected two read attempts, got %d", client.readCalls)
	}
}

func TestReadRangePrefetchesAndReusesCachedWindow(t *testing.T) {
	client := &stubClient{}
	r := NewReader(client)
	got, err := r.ReadRange("fsid-1", 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(got))
	}
	got, err = r.ReadRange("fsid-1", 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", len(got))
	}
	if client.readCalls != 1 {
		t.Fatalf("expected one backend read due to prefetch cache, got %d", client.readCalls)
	}
}
