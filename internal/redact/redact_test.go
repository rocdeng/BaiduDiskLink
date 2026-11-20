package redact

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestErrorRemovesQueryButKeepsCause(t *testing.T) {
	raw := &url.Error{Op: "Get", URL: "https://pan.baidu.com/rest?access_token=SECRET&x=1", Err: context.DeadlineExceeded}
	for _, err := range []error{raw, fmt.Errorf("list: %w", raw)} {
		got := Error(err)
		if strings.Contains(got.Error(), "SECRET") {
			t.Fatalf("credential leaked: %v", got)
		}
		if !strings.Contains(got.Error(), "pan.baidu.com/rest") {
			t.Fatalf("host/path should stay for diagnostics: %v", got)
		}
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("cause lost: %v", got)
		}
	}
}

func TestErrorPassesThroughNonURLErrors(t *testing.T) {
	plain := errors.New("plain")
	if Error(plain) != plain || Error(nil) != nil {
		t.Fatal("non-url errors must pass through unchanged")
	}
}
