# zfsbackrest

> **⚠️ Experimental:**  
> Do not use it as your only way for backups. This is something I wrote over a
> weekend. There's a lot of things that need work here.

[pgbackrest](https://pgbackrest.org/) style encrypted backups for ZFS
filesystems.

## Getting Started

### Installing

You need [age](https://github.com/FiloSottile/age) installed to generate
encryption keys. Encryption is NOT optional.

```bash
$ go install github.com/gargakshit/zfsbackrest/cmd/zfsbackrest@latest
```

### Configuring

Create `/etc/zfsbackrest.toml`.

```toml
debug = true # warning, may log sensitive data

[repository]
# zfsbackrest does not support changing the list of datasets after a repository
# is initialized YET. That's one feature I need.
included_datasets = ["storage/*"] # Glob is supported

[repository.s3]
# zfsbackrest does NOT support non-secure S3 endpoints.
endpoint = "todo"
bucket = "todo"
key = "todo"
secret = "todo"
region = "todo"

[repository.expiry]
# Child backups expire if the parent expires. See the model below for a better
# explanation.
full = "336h" # 14 days
diff = "120h" # 5 days
incr = "24h" # 1 day

[upload_concurrency]
full = 2
diff = 4
incr = 4
```

### Creating a repository

```bash
$ zfsbackrest init --age-recipient-public-key="<your age public key>"
```

### Backing up

```bash
$ zfsbackrest backup --type <full | diff | incr>
```

`full` backups are standalone. They do not depend on any other backups. They are
also huge in size because of that.

`diff` backups are sent incrementally from the latest `full` backup. They depend
on the parent `full` backup to be present in the repository to restore.

`incr` backups are send incrementally from the latest `diff` backup. They depend
on the parent `diff` backup to restore.

### Viewing the repository

```bash
$ zfsbackrest detail
```

It shows a list of backups, orphans and all.

### Cleaning up the repository

Sometimes, orphaned backups are left as an artefact of incomplete or cancelled
backups. You can clean those by running

```bash
$ zfsbackrest cleanup --orphans --dry-run=false
```

You can clean up expired backups by running

```bash
$ zfsbackrest cleanup --expired --dru-run=false
```

### Restoring

To restore the backups, you'll need your age identity file (private key).

```bash
zfsbackrest restore -i <path-to-age-identity-file> \
  -s <name of the dataset to restore from> \
  -b <optionally, the backup ID to restore from, leave empty to restore the latest> \
  -d <name of the dataset to restore to> # Restoring to a dataset that already exists on your local FS will fail.
```

## Safety

`zfsbackrest` doesn't write or modify actual `zfs` datasets. It makes extensive
use of snapshots. List of `zfs` operations used by `zfsbackrest` are

- `backup`

  - `zfs snapshot` - Creating a `zfs` snapshot for `zfsbackrest`
  - `zfs hold` - Creating a reference to that snapshot to prevent removal
  - `zfs send` - Sending the snapshot incrementally

- `cleanup` / `force-destroy`

  - `zfs release` - Release the held snapshot
  - `zfs destroy` - Destroy the snapshot

- `restore`
  - `zfs recv` - Receiving the remote snapshot

## Model

TODO
