#!/usr/bin/env bash
# Builds the companion fork's npm artifacts for one platform, non-interactively,
# the way .github/workflows/release.yml does (see COMPANION-BUILD.md).
#
#   scripts/companion-build.sh                     # linux-x64 into ./companion-artifacts
#   T3_COMPANION_TARGET=darwin-arm64 scripts/companion-build.sh
#
# Environment:
#   T3_COMPANION_TARGET            <os>-<arch> in Node naming: linux-x64 (default),
#                                  linux-arm64, darwin-arm64, win32-x64, win32-arm64.
#   T3_COMPANION_OUT               output directory (default: <repo>/companion-artifacts).
#   T3_COMPANION_BASE_VERSION      upstream version the fork sits on (default: nearest
#                                  reachable v* tag, e.g. 0.0.43-nightly.20260916.1825).
#   T3_COMPANION_VERSION           full artifact version (default:
#                                  <base>-rm-async-delivery.<short sha of HEAD>).
#   T3_COMPANION_RESOURCE_MONITOR  "upstream" (default): take the Rust resource monitor
#                                  binary for the target from upstream's GitHub release
#                                  archive of the base version, verified against its
#                                  SHA256SUMS. "none": ship without it (the server then
#                                  degrades resource telemetry). Or a directory laid out
#                                  like resource-monitor/<platformKey>/t3-resource-monitor.
#   T3_COMPANION_NODE_PTY          "upstream" (default) or "none". node-pty has no Linux
#                                  prebuild and compiles from source, which a non-Linux
#                                  host cannot do for a Linux target; "upstream" copies the
#                                  compiled binary from the same upstream release archive.
#   T3_COMPANION_SKIP_INSTALL=1    skip `pnpm install` (deps already present).
#   T3_COMPANION_SKIP_SMOKE=1      skip the host-only archive smoke test.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

log() { printf '[companion-build] %s\n' "$*" >&2; }
die() { log "error: $*"; exit 1; }

PLATFORM_KEY="${T3_COMPANION_TARGET:-linux-x64}"
case "$PLATFORM_KEY" in
  linux-x64)    ARCHIVE_PLATFORM=linux; ARCH=x64;   EXE_TARGET=linux-x64 ;;
  linux-arm64)  ARCHIVE_PLATFORM=linux; ARCH=arm64; EXE_TARGET=linux-arm64 ;;
  darwin-arm64) ARCHIVE_PLATFORM=mac;   ARCH=arm64; EXE_TARGET=darwin-arm64 ;;
  win32-x64)    ARCHIVE_PLATFORM=win;   ARCH=x64;   EXE_TARGET=win-x64 ;;
  win32-arm64)  ARCHIVE_PLATFORM=win;   ARCH=arm64; EXE_TARGET=win-arm64 ;;
  *) die "unsupported T3_COMPANION_TARGET '$PLATFORM_KEY'" ;;
esac
ARCHIVE_EXT=tar.gz
[[ "$ARCHIVE_PLATFORM" == win ]] && ARCHIVE_EXT=zip

HOST_KEY="$(node -p 'process.platform + "-" + process.arch')"
BASE_VERSION="${T3_COMPANION_BASE_VERSION:-$(git describe --tags --match 'v*' --abbrev=0 | sed 's/^v//')}"
SHORT_SHA="$(git rev-parse --short HEAD)"
VERSION="${T3_COMPANION_VERSION:-${BASE_VERSION}-rm-async-delivery.${SHORT_SHA}}"
OUT_DIR="${T3_COMPANION_OUT:-$ROOT/companion-artifacts}"
RESOURCE_MONITOR="${T3_COMPANION_RESOURCE_MONITOR:-upstream}"
NODE_PTY_SOURCE="${T3_COMPANION_NODE_PTY:-upstream}"
WORK_DIR="$ROOT/.companion-build"
RELEASE_CLI_DIR="$WORK_DIR/release-cli"
NPM_PACKAGES_DIR="$WORK_DIR/npm-packages"

log "target=$PLATFORM_KEY host=$HOST_KEY base=$BASE_VERSION version=$VERSION"
log "out=$OUT_DIR"

# The release workflow rewrites these manifests to the release version before
# building and never commits them; restore them whatever happens.
VERSIONED_MANIFESTS=(apps/server/package.json apps/desktop/package.json apps/web/package.json packages/contracts/package.json)
restore_manifests() { git checkout -q -- "${VERSIONED_MANIFESTS[@]}" || true; }
trap restore_manifests EXIT

command -v pnpm >/dev/null || die "pnpm is required (corepack enable; see packageManager in package.json)"
# The build scripts spawn `vp` (Vite+) directly; make the workspace's copy
# resolvable without a global install.
export PATH="$ROOT/node_modules/.bin:$PATH"
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
(( NODE_MAJOR >= 25 )) || die "Node 25.7+ is required on the build host for --build-sea (found $(node -v)); the Node embedded in the executable is pinned separately in apps/server/vite.config.ts"

if [[ "${T3_COMPANION_SKIP_INSTALL:-0}" != 1 ]]; then
  log "installing dependencies (release.yml filters)"
  pnpm install --frozen-lockfile --filter=t3... --filter=@t3tools/web... --filter=@t3tools/scripts...
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "$OUT_DIR"

log "aligning package versions to $VERSION"
node scripts/update-release-package-versions.ts "$VERSION"

# release.yml: "Build JS bundle" (t3#build runs @t3tools/web#build first, then
# `node scripts/cli.ts build` = `vp pack` + web client copy into apps/server/dist).
log "building web client and server bundle"
pnpm exec vp run --filter t3 build

# release-desktop.yml: "Build CLI single-executable" for the target.
log "building single-executable for $EXE_TARGET"
node apps/server/scripts/cli.ts build-exe --target "$EXE_TARGET" --verbose

# Upstream's release archive for the same base version and target, fetched
# once and verified against its SHA256SUMS. Native pieces that cannot be
# cross-built here are taken from it; their sources are identical at that tag.
UPSTREAM_CONTENT_DIR=""
fetch_upstream_archive() {
  [[ -n "$UPSTREAM_CONTENT_DIR" ]] && return 0
  local archive="t3-${BASE_VERSION}-${PLATFORM_KEY}.${ARCHIVE_EXT}"
  local base="https://github.com/pingdotgg/t3code/releases/download/v${BASE_VERSION}"
  local dir="$WORK_DIR/upstream"
  mkdir -p "$dir"
  log "fetching upstream $archive"
  curl -fsSL -o "$dir/SHA256SUMS" "$base/SHA256SUMS"
  curl -fsSL -o "$dir/$archive" "$base/$archive"
  local expected actual
  expected="$(awk -v f="$archive" '$2 == f || $2 == "*" f { print $1 }' "$dir/SHA256SUMS")"
  [[ -n "$expected" ]] || die "$archive is not listed in upstream SHA256SUMS"
  if command -v sha256sum >/dev/null; then actual="$(sha256sum "$dir/$archive" | awk '{print $1}')"; else actual="$(shasum -a 256 "$dir/$archive" | awk '{print $1}')"; fi
  [[ "$actual" == "$expected" ]] || die "checksum mismatch for $archive"
  tar -xf "$dir/$archive" -C "$dir"
  UPSTREAM_CONTENT_DIR="$dir/t3-${BASE_VERSION}-${PLATFORM_KEY}"
  [[ -d "$UPSTREAM_CONTENT_DIR" ]] || die "unexpected layout in upstream $archive"
}

# Resource monitor (Rust). Cross-compiling it needs a Rust toolchain per target,
# so by default reuse upstream's binary for the same base version.
RESOURCE_MONITOR_DIR="$WORK_DIR/resource-monitor"
mkdir -p "$RESOURCE_MONITOR_DIR"
case "$RESOURCE_MONITOR" in
  none)
    log "shipping without a resource monitor binary (T3_COMPANION_RESOURCE_MONITOR=none)"
    ;;
  upstream)
    fetch_upstream_archive
    cp -R "$UPSTREAM_CONTENT_DIR/resource-monitor/." "$RESOURCE_MONITOR_DIR/"
    log "resource monitor staged from upstream: $(find "$RESOURCE_MONITOR_DIR" -type f | sed "s|$RESOURCE_MONITOR_DIR/||" | tr '\n' ' ')"
    ;;
  *)
    [[ -d "$RESOURCE_MONITOR" ]] || die "T3_COMPANION_RESOURCE_MONITOR '$RESOURCE_MONITOR' is not a directory"
    cp -R "$RESOURCE_MONITOR/." "$RESOURCE_MONITOR_DIR/"
    ;;
esac

# release-desktop.yml: "Build CLI archive". scripts/build-cli-archive.ts passes
# GNU tar's --hard-dereference for Linux archives; macOS ships bsdtar, which
# rejects the flag, so a Darwin host gets a tar shim that drops it (pnpm clones
# rather than hard-links on APFS, so nothing is lost).
if [[ "$(uname -s)" == Darwin ]]; then
  SHIM_DIR="$WORK_DIR/tar-shim"
  mkdir -p "$SHIM_DIR"
  cat > "$SHIM_DIR/tar" <<'SHIM'
#!/usr/bin/env bash
args=()
for a in "$@"; do [[ "$a" == --hard-dereference ]] || args+=("$a"); done
exec /usr/bin/tar "${args[@]}"
SHIM
  chmod +x "$SHIM_DIR/tar"
  export PATH="$SHIM_DIR:$PATH"
fi
log "building CLI archive"
node scripts/build-cli-archive.ts \
  --platform "$ARCHIVE_PLATFORM" --arch "$ARCH" --version "$VERSION" \
  --resource-monitor-dir "$RESOURCE_MONITOR_DIR" --output-dir "$RELEASE_CLI_DIR"
ARCHIVE_FILE="t3-${VERSION}-${PLATFORM_KEY}.${ARCHIVE_EXT}"

# node-pty ships no Linux prebuild: pnpm compiles it with node-gyp during the
# staging install, which only works when the host is the target. For a cross
# build, take the compiled binary from upstream's archive (same node-pty
# version). In every case mirror build/Release into prebuilds/<platformKey>/:
# node-pty's install script only checks that this directory exists, so a later
# `npm install` of the package does not need a compiler on the target.
if [[ "$ARCHIVE_PLATFORM" != win ]]; then
  log "repairing node-pty in the archive"
  REPACK_DIR="$WORK_DIR/repack"
  rm -rf "$REPACK_DIR" && mkdir -p "$REPACK_DIR"
  tar -xzf "$RELEASE_CLI_DIR/$ARCHIVE_FILE" -C "$REPACK_DIR"
  STEM="t3-${VERSION}-${PLATFORM_KEY}"
  PTY_DIR="$REPACK_DIR/$STEM/node_modules/node-pty"
  [[ -d "$PTY_DIR" ]] || die "node-pty missing from the archive"
  if [[ ! -f "$PTY_DIR/build/Release/pty.node" ]]; then
    [[ "$NODE_PTY_SOURCE" == upstream ]] || die "node-pty has no compiled binary and T3_COMPANION_NODE_PTY=$NODE_PTY_SOURCE"
    fetch_upstream_archive
    UPSTREAM_PTY_DIR="$UPSTREAM_CONTENT_DIR/node_modules/node-pty"
    ours="$(node -p 'require(process.argv[1]).version' "$PTY_DIR/package.json")"
    theirs="$(node -p 'require(process.argv[1]).version' "$UPSTREAM_PTY_DIR/package.json")"
    [[ "$ours" == "$theirs" ]] || die "node-pty version differs from upstream ($ours vs $theirs); cannot reuse its binary"
    [[ -f "$UPSTREAM_PTY_DIR/build/Release/pty.node" ]] || die "upstream archive has no compiled node-pty either"
    mkdir -p "$PTY_DIR/build/Release"
    cp -R "$UPSTREAM_PTY_DIR/build/Release/." "$PTY_DIR/build/Release/"
    log "node-pty $ours binary taken from upstream: $(ls "$PTY_DIR/build/Release" | tr '\n' ' ')"
  fi
  mkdir -p "$PTY_DIR/prebuilds/$PLATFORM_KEY"
  cp -R "$PTY_DIR/build/Release/." "$PTY_DIR/prebuilds/$PLATFORM_KEY/"
  rm -f "$RELEASE_CLI_DIR/$ARCHIVE_FILE"
  tar -czf "$RELEASE_CLI_DIR/$ARCHIVE_FILE" -C "$REPACK_DIR" "$STEM"
fi

if [[ "$HOST_KEY" == "$PLATFORM_KEY" && "${T3_COMPANION_SKIP_SMOKE:-0}" != 1 ]]; then
  log "smoke-testing the archive on this host"
  node scripts/smoke-cli-archive.ts --archive "$RELEASE_CLI_DIR/$ARCHIVE_FILE" --expect-version "$VERSION"
else
  log "skipping archive smoke test (host $HOST_KEY != target $PLATFORM_KEY); run it on a $PLATFORM_KEY box"
fi

# release.yml: "Build npm packages from CLI archives". --allow-missing because
# only one platform is built; the launcher lists just that platform.
log "building npm packages"
node scripts/build-npm-platform-packages.ts \
  --archives-dir "$RELEASE_CLI_DIR" --version "$VERSION" --output-dir "$NPM_PACKAGES_DIR" --allow-missing

# Upstream publishes the platform package to npm and the launcher pulls it as
# an optional dependency. Nothing here is published, so also produce a
# self-contained launcher tarball with the platform package bundled inside
# (npm keeps node_modules entries listed in bundleDependencies), giving
#   node_modules/t3/dist/bin.mjs
#   node_modules/t3/node_modules/@t3code/t3-<platform>/t3
# from a single `npm install -g --prefix <prefix> ./t3-<version>.tgz`.
log "bundling the platform package into the launcher tarball"
BUNDLE_STAGE="$WORK_DIR/bundle"
mkdir -p "$BUNDLE_STAGE"
tar -xzf "$NPM_PACKAGES_DIR/t3.tgz" -C "$BUNDLE_STAGE"
PLATFORM_PKG_DIR="$BUNDLE_STAGE/package/node_modules/@t3code/t3-${PLATFORM_KEY}"
mkdir -p "$PLATFORM_PKG_DIR"
tar -xzf "$NPM_PACKAGES_DIR/@t3code/t3-${PLATFORM_KEY}.tgz" -C "$PLATFORM_PKG_DIR" --strip-components=1
node -e '
const fs = require("node:fs");
const file = process.argv[1];
const manifest = JSON.parse(fs.readFileSync(file, "utf8"));
manifest.bundleDependencies = Object.keys(manifest.optionalDependencies ?? {});
fs.writeFileSync(file, JSON.stringify(manifest, null, 2) + "\n");
' "$BUNDLE_STAGE/package/package.json"

rm -f "$OUT_DIR"/t3-*.tgz "$OUT_DIR"/t3code-t3-*.tgz "$OUT_DIR"/t3-*-"$PLATFORM_KEY".* "$OUT_DIR/SHA256SUMS"
tar -czf "$OUT_DIR/t3-${VERSION}.tgz" -C "$BUNDLE_STAGE" package
cp "$NPM_PACKAGES_DIR/t3.tgz" "$OUT_DIR/t3-launcher-only-${VERSION}.tgz"
cp "$NPM_PACKAGES_DIR/@t3code/t3-${PLATFORM_KEY}.tgz" "$OUT_DIR/t3code-t3-${PLATFORM_KEY}-${VERSION}.tgz"
cp "$RELEASE_CLI_DIR/$ARCHIVE_FILE" "$OUT_DIR/$ARCHIVE_FILE"
(
  cd "$OUT_DIR"
  if command -v sha256sum >/dev/null; then sha256sum t3-*.tgz t3code-t3-*.tgz "$ARCHIVE_FILE" > SHA256SUMS; else shasum -a 256 t3-*.tgz t3code-t3-*.tgz "$ARCHIVE_FILE" > SHA256SUMS; fi
)

log "done"
log "artifacts in $OUT_DIR:"
ls -l "$OUT_DIR" >&2
log "install: npm install -g --prefix <prefix> $OUT_DIR/t3-${VERSION}.tgz"
