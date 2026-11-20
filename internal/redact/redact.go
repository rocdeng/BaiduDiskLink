// Package redact 负责在错误信息进入日志前去除 URL 中的凭证。
//
// Go 的 *url.Error 会把完整请求 URL（含 access_token、refresh_token、
// client_secret、CDN 签名等 query 参数）拼进 Error()，这里统一替换掉。
package redact

import (
	"errors"
	"net/url"
	"strings"
)

const placeholder = "REDACTED"

// URL 返回去掉 query、fragment 与 userinfo 后的 URL 字符串。
func URL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return placeholder
	}
	if u.RawQuery != "" {
		u.RawQuery = placeholder
	}
	u.Fragment = ""
	u.RawFragment = ""
	u.User = nil
	return u.String()
}

// Error 如果 err 链中包含 *url.Error，返回一个 URL 已脱敏的等价错误。
// 返回的错误仍可通过 errors.Is/As 匹配原始底层错误（如 context.DeadlineExceeded）。
func Error(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	if urlErr == err {
		return &url.Error{Op: urlErr.Op, URL: URL(urlErr.URL), Err: urlErr.Err}
	}
	// url.Error 被包装在其他错误中时，无法安全地重建外层，直接用脱敏信息替换。
	return &wrapped{msg: replaceURL(err.Error(), urlErr.URL), cause: urlErr.Err}
}

type wrapped struct {
	msg   string
	cause error
}

func (w *wrapped) Error() string { return w.msg }
func (w *wrapped) Unwrap() error { return w.cause }

func replaceURL(message, raw string) string {
	if raw == "" {
		return message
	}
	return strings.ReplaceAll(message, raw, URL(raw))
}
