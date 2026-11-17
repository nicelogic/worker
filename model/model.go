package model

import "github.com/eclipse/paho.golang/paho"

type MqCfg struct {
	Urls                          []string
	Name                          string
	Pwd                           string
	Handlers                      map[string]paho.MessageHandler
	ReportWorkerStatusTopicFormat string
}

type WorkerCfg struct {
	Type     string `json:"type"`
	Id       string `json:"id"`
	MaxTasks int    `json:"maxTasks"`
}

type WorkerStatus struct {
	Id       string `json:"id"`
	MaxTasks int    `json:"maxTasks"`
	Online   bool   `json:"online"`
}

type RegisterResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}
