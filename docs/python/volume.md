# Volume and inputs

A session has one writable directory and zero or more read-only inputs (PROTOCOL §9.1.4). The SDK does not load them at startup. The strategy calls `read` and `write` itself, usually from `on_init` and from `on_stop`.

`volume_dir` is the writable directory. The platform creates it before the first incarnation and keeps it for later incarnations of the same session. `input_dir` is the directory of one named input from the spec. Inputs are unpacked once per session. Files there are read-only. Do not write them. Do not write anywhere except the volume directory and the system temporary directory, and do not override `MFTIKO_VOLUME_DIR` or `MFTIKO_INPUT_DIR`. The context methods already return the paths the platform prepared.

## Read order

`read` takes the volume directory, the input directory, and a file `name`. It returns the file's bytes, or `None` when neither directory has it. The volume copy wins, including a volume file that is empty. If the volume file is absent, the input file is used. If both are absent, the strategy starts from scratch. That is the whole restore rule. There is no automatic snapshot of strategy objects.

An empty directory string is skipped. It is not replaced with the process's current directory. `name` is still checked. A name that is empty, absolute, or not already a clean relative path is an error (`volume: name must be a relative path inside the directory`), even when both directories are empty. Use `/` as the separator. A name may contain intermediate directories (`nest/leaf`). `.` and `..` are rejected, including a `..` reached by cleaning the path. `input_dir` uses the same rule and returns an empty string for a name it rejects, so pass a safe relative name to both.

An I/O error other than a missing file stops the search and raises `OSError`. The second directory is not tried after a permission error on the first.

`stateful` restores a fill count this way: `read` of the volume directory, then the `state` input, then zero when both are absent. The timer and `on_stop` replace that file with `write`. See [Examples](examples.md).

## Atomic replace

`write` replaces `dir`/`name` with `data`. `dir` and the directory arguments of `read` accept a string or any `os.PathLike` of a string. A bytes path raises `TypeError` (`volume path must be a str, not bytes`). `data` must be `bytes`, `bytearray`, or another bytes-like buffer. A `str` raises `TypeError` (`volume data must be bytes-like, not str`). The SDK does not encode text. Encode it in the strategy if the file is text.

The replace is atomic. A temporary file is created in the destination directory, written, synced, closed, renamed onto `name`, and the directory is synced. A reader sees the previous complete file or the new bytes, never a prefix of the new bytes. A failure before the rename deletes the temporary file when it can. A crash may leave a temporary file whose name starts with `.vol-`. That file is not the saved state. An empty `dir` raises `OSError` (`volume: empty directory`) and writes nothing.

`write` and `read` release the GIL during the filesystem calls. They still occupy the hook until they return. Keep the files small. The account and market-data hooks after start should not do large copies. `on_init`, `on_stop`, and a timer are the usual places to persist state.

## Pretrained models

A model, a parameter table, or any other file the strategy only reads goes in an input. It is uploaded once, before any session uses it, as a data artifact. mftiko's `mftiko-pack data` uploads a directory and prints the manifest digest on stdout. A summary goes to stderr.

```bash examples/python/pretrained/README.md:11-11
mftiko-pack data -dir ./model -repo registry.example/acme/models/abc -tag v1
```

`-repo` is the full path, registry host included. The registry credentials come from Docker's config file, as for `mftik.pack`. `-tag` is optional and is only a name a person can look up. The session refers to the digest. The same tree uploads to the same digest, and blobs the registry already has are not sent again. The upload checks the limits before it sends anything: 1 GiB compressed, 4 GiB unpacked, and 100,000 entries. Weights hardly compress, so 1 GiB is the limit that matters. The directory may hold regular files, directories, and relative symlinks that stay inside it. Any other entry is an error that names its path. `-L` copies what each symlink points to instead. A Hugging Face cache, whose `snapshots` link into `../../blobs`, needs it. The tool ships with the `mftiko` CLI. `mftiko-pack data -h` lists its flags.

The session file names the digest under `inputs`. The input's repository has to be on the API's allowed list, and its size under the API's limit. See [Session spec](session.md#inputs).

```yaml examples/python/pretrained/session.yaml:6-9
inputs:
  - name: model
    repository: registry.example/acme/models/abc
    digest: sha256:4d1f0c2b9a8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a392817060f5e
```

The platform unpacks the input before the process starts. Every session unpacks its own copy. Files are mode 0444 and directories 0555, so a write there fails with `PermissionError`. Read the model in `on_init`, which has the session's `start_timeout_s` to finish. An exception there ends the session `INIT_FAILED`.

```python examples/python/pretrained/pretrained.py:42-57
    def on_init(self, ctx: mftik.Context) -> None:
        directory = ctx.input_dir(_INPUT)
        if not directory:
            raise RuntimeError("pretrained: the session has no model input")
        raw = (Path(directory) / _FILE).read_bytes()
        weights = json.loads(raw)
        if not isinstance(weights, dict):
            raise ValueError(f"pretrained: {_FILE} must be a JSON object")
        self._weights = weights
        _log(
            {
                "hook": "OnInit",
                "keys": sorted(weights),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
```

A model that the strategy updates while it runs is state, not an input. Keep that in the volume, and start the next session from its `state_out`.

## The platform's limit

The SDK does not enforce a quota. The platform measures the volume directory and, when it exceeds `Limits.volume_mb` (a zero in the spec means the platform default, 1024 MiB), stops the session with outcome `FAILED` and reason `volume_over_limit`. The session is not restarted for that reason. Write only what the next incarnation needs.

After the session is terminal and the process has exited, the platform packs a non-empty volume and publishes it. An empty volume is discarded. The strategy does not call that packer. `python -m mftik.pack` builds the code image, not the volume.
