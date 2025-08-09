package config

type S3Store struct {
	Endpoint string
	Bucket   string
	Key      string
	Secret   string
	Region   string
}
