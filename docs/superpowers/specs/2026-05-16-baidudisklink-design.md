# BaiduDiskLink Design

## Goal

BaiduDiskLink maps Baidu Netdisk into a Synology DSM local directory for video media library use. The first version is a read-only filesystem for Emby. It should avoid the WebDAV-to-share-folder workflow used by OpenList-style deployments and expose a direct local mount instead.

The product is optimized for:

- Synology DSM, including Linux-based native execution and Docker deployment.
- Video media libraries scanned and played by Emby.
- Local-only credential storage.
- Metadata caching without full file downloads.

## Non-Goals

The first version will not support:

- Uploading, deleting, renaming, or moving files.
- WebDAV, SMB, or HTTP gateway output.
- Full file mirroring or full video download cache.
- Multi-user permission systems.
- Cloud relay services.
- Real-time bidirectional sync.

## Architecture

The system has four core modules:

1. Auth Manager
   Handles login, credential refresh, and encrypted local credential storage.

2. Metadata Store
   Stores directory and file metadata in SQLite. It does not store file content.

3. FUSE Filesystem
   Exposes Baidu Netdisk as a read-only local filesystem mounted on DSM.

4. Remote Fetcher
   Reads file content on demand from Baidu Netdisk and serves FUSE read requests.

OpenList is used as a compatibility reference for Baidu Netdisk behavior, especially directory listing, metadata fields, download link acquisition, request headers, error handling, and large-file playback behavior. BaiduDiskLink should not copy OpenList's overall WebDAV/Web server architecture.

## Deployment Model

The recommended first deployment target is Docker on DSM.

Docker is preferred for the MVP because it is faster to test, easier to distribute, and avoids early DSM package complexity. The container will need access to FUSE, likely through `/dev/fuse` and suitable container permissions. The mounted path will be bind-mounted to a DSM-visible directory, such as `/volume1/BaiduDisk`.

A native DSM package can be considered after the Docker version proves stable.

## Metadata Store

SQLite stores the local view of the remote filesystem.

Suggested fields:

- `id`: local primary key.
- `fs_id`: Baidu Netdisk file identifier.
- `parent_fs_id`: parent directory identifier.
- `path`: normalized absolute remote path.
- `name`: file or directory name.
- `is_dir`: directory flag.
- `size`: byte size for files.
- `mtime`: remote modified time.
- `md5`: remote md5 when available.
- `mime_type`: optional media type.
- `etag` or `version`: remote change marker when available.
- `last_sync_at`: local sync timestamp.
- `expires_at`: cache expiry timestamp.
- `negative`: marks short-lived negative cache entries.

Metadata caching strategy:

- On first directory access, fetch the remote directory listing and persist it.
- Subsequent directory reads use SQLite while the entry is fresh.
- Stale directories are refreshed lazily or by a background worker.
- Empty directories and missing paths use short-lived negative cache entries to reduce repeated remote calls during Emby scans.
- The first version accepts eventual consistency. Stable scanning and playback are more important than instant cloud-side updates.

## FUSE Behavior

The filesystem is read-only.

Supported operations:

- `lookup`
- `getattr`
- `readdir`
- `open`
- `read`
- `release`

Unsupported write operations should return read-only filesystem errors:

- `write`
- `mkdir`
- `unlink`
- `rename`
- `rmdir`
- `chmod`
- `chown`
- `truncate`

Directory operations should primarily read from SQLite. If a directory is missing or stale, the FUSE layer asks the metadata service to refresh it.

File reads are forwarded to the Remote Fetcher.

## Video Read Path

The read path must support Emby-style media access:

- file probing
- small header reads
- random seek
- continuous sequential playback
- retry after transient network failures

The intended flow is:

```text
Emby
  -> DSM local mount
  -> FUSE read/open/seek
  -> Remote Fetcher
  -> Baidu Adapter
  -> Baidu Netdisk download endpoint
```

The first version should not store complete files locally. It should use a small read buffer and optional block cache for recent byte ranges.

The Baidu Adapter should support range requests when available. Large video files may require specific request headers; OpenList's Baidu implementation should be used as the reference for these details.

## Baidu Adapter

The Baidu Adapter hides all Baidu-specific behavior behind a small internal interface.

Suggested interface:

- `list(path or fs_id) -> []RemoteEntry`
- `stat(path or fs_id) -> RemoteEntry`
- `getDownloadLink(fs_id) -> DownloadLink`
- `readRange(fs_id, offset, length) -> bytes`
- `refreshAuth()`

Reference areas from OpenList:

- Directory listing request shape and pagination.
- Metadata fields such as `fs_id`, `server_filename`, `path`, `size`, `md5`, `isdir`, `server_mtime`, and `local_mtime`.
- Download link acquisition.
- Required headers for large files and video playback.
- Range request behavior.
- Token, cookie, and session expiry handling.
- Baidu error code mapping.
- Retry and throttling behavior.

Because OpenList is AGPL-3.0, BaiduDiskLink should treat it as a behavior reference unless the project intentionally adopts a compatible license.

## Auth And Security

Credentials must remain local to the DSM machine.

The first version should:

- Avoid storing the user's Baidu password.
- Store only the minimum required token, cookie, or session data.
- Encrypt credential storage where practical.
- Provide a clear way to revoke or reset local credentials.
- Avoid any cloud relay or third-party backend.

The exact login method should be chosen after validating current Baidu Netdisk API behavior. Preferred options are QR login, device-code-style login, or browser-assisted login that produces local session material.

## Background Jobs

The background worker handles:

- Periodic metadata refresh.
- Retry of failed directory refreshes.
- Optional hot-directory refresh for Emby library paths.
- Cleanup of expired negative cache entries.
- Cleanup of old read-buffer blocks if block caching is enabled.

Background work should be conservative to avoid triggering Baidu rate limits.

## Configuration

Initial configuration should be small:

- Mount path.
- Metadata database path.
- Credential storage path.
- Refresh interval.
- Maximum read buffer or block cache size.
- Remote root path to expose.
- Log level.

For Emby-focused use, allowing the user to expose only selected media directories is better than mounting the entire netdisk by default.

## Testing Strategy

Key test areas:

- Metadata normalization and path handling.
- SQLite cache freshness and negative cache behavior.
- FUSE read-only error behavior.
- Directory listing for large folders.
- File stat consistency for Emby scanning.
- Range read correctness.
- Retry behavior for transient remote failures.
- Token/session expiry behavior.

DSM integration tests should verify:

- Docker container can mount through FUSE.
- DSM can see the bind-mounted directory.
- Emby can scan the mounted library.
- Emby can start playback and seek within large video files.

## Open Questions

- Which login flow is most reliable for Baidu Netdisk today?
- Which Baidu download endpoint should be the default for stable video playback?
- How much local block caching is useful before it becomes unwanted downloading?
- What DSM versions and CPU architectures should the first Docker image support?
- Should the first version expose one configured remote root or multiple named roots?

## Recommended MVP

Build the first version as a Dockerized, read-only FUSE filesystem for DSM:

- Single Baidu account.
- Single configured remote root.
- SQLite metadata cache.
- Lazy directory loading.
- Short-lived negative cache.
- Official or most stable Baidu download path.
- Range reads for video playback.
- Small bounded read buffer.
- No WebDAV.
- No write operations.
- No full content cache.

