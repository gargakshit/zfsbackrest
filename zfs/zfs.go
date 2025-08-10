package zfs

type ZFS struct{}

func New() (*ZFS, error) {
	return &ZFS{}, nil
}
