package config

type UploadConcurrency struct {
	Full int `json:"full"`
	Diff int `json:"diff"`
	Incr int `json:"incr"`
}
