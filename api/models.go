package api

type Source struct {
	SourceType SourceType    `json:"sourceType"`
	Details    SourceDetails `json:"details"`
}

type SourceType struct {
	IsLive bool `json:"isLive"`
}

type SourceDetails struct {
	URL            string `json:"url"`
	RadioStreamURL string `json:"radioStreamUrl"`
}
