package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nicelogic/worker/model"
	"github.com/nicelogic/worker/mq"
)

type Worker struct {
	cfg      *model.WorkerCfg
	Mq       *mq.Mq
	Registed bool
}

func (worker *Worker) Init(
	workerCfg *model.WorkerCfg,
	mqCfg *model.MqCfg,
	ctx context.Context,
) error {
	//check part
	if workerCfg == nil || workerCfg.Type == "" {
		return fmt.Errorf("workerCfg.Type is empty")
	}
	if workerCfg.Id == "" {
		return fmt.Errorf("workerCfg.Id is empty")
	}
	if workerCfg.MaxTasks <= 0 {
		return fmt.Errorf("workerCfg.MaxTasks must be greater than zero")
	}
	if mqCfg == nil {
		return fmt.Errorf("mqCfg is nil")
	}
	if len(mqCfg.Urls) == 0 {
		return fmt.Errorf("mqCfg.Urls is empty")
	}
	if mqCfg.Name == "" || mqCfg.Pwd == "" {
		return fmt.Errorf("mqtt credentials are empty")
	}
	if len(mqCfg.Handlers) == 0 {
		return fmt.Errorf("mqCfg.Handlers is empty")
	}
	//====

	worker.cfg = workerCfg
	router := paho.NewStandardRouter()
	router.DefaultHandler(func(p *paho.Publish) {
		slog.Info("mqtt default handler", "topic", p.Topic)
	})

	subTopics := make([]string, 0, len(mqCfg.Handlers))
	for topic, handler := range mqCfg.Handlers {
		subTopics = append(subTopics, topic)
		router.RegisterHandler(topic, handler)
	}

	reportWorkerStatusTopic := fmt.Sprintf(mqCfg.ReportWorkerStatusTopicFormat, worker.cfg.Id)
	worker.Mq = &mq.Mq{}
	if err := worker.Mq.Init(
		worker.cfg.Id,
		mqCfg.Urls,
		mqCfg.Name,
		mqCfg.Pwd,
		ctx,
		subTopics,
		router,
		reportWorkerStatusTopic,
	); err != nil {
		return fmt.Errorf("mq init: %w", err)
	}

	// 监听连接事件，连接成功就注册。
	// 重连成功同样会触发注册。
	go worker.run(ctx)

	return nil
}

// 等待连接成功，然后执行注册。
func (worker *Worker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case connected := <-worker.Mq.Ready:
			if !connected {
				slog.Warn("worker mqtt disconnected, broker may send will msg to notify offline")
				continue
			}

			slog.Info("worker mqtt ready, start registering")

			worker.registerWithRetry(ctx)
		}
	}
}

// 注册失败自动重试，最大间隔 30 秒。
func (worker *Worker) registerWithRetry(ctx context.Context) {
	delay := 2 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		if err := worker.register(ctx); err == nil {
			slog.Info("worker registered successfully",
				"id", worker.cfg.Id,
			)
			return
		} else {
			slog.Error("worker register failed",
				"id", worker.cfg.Id,
				"err", err,
				"retryAfter", delay,
			)
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
}

// MQTT 5 RPC 注册。
func (worker *Worker) register(ctx context.Context) error {

	payload, err := json.Marshal(model.WorkerStatus{
		Id:       worker.cfg.Id,
		Version:  worker.cfg.Version,
		MaxTasks: worker.cfg.MaxTasks,
		Online:   true,
	})
	if err != nil {
		return err
	}
	respPayload, err := worker.Mq.RpcRequest(
		ctx,
		&paho.Publish{
			QoS:     0,
			Topic:   worker.Mq.ReportWorkerStatusTopic,
			Payload: payload,
			Retain:  true,
		},
		10,
	)
	if err != nil {
		return fmt.Errorf("register rpc: %w", err)
	}
	var response model.RegisterResponse
	if err := json.Unmarshal(respPayload, &response); err != nil {
		return fmt.Errorf("unmarshal register response: %w", err)
	}
	if !response.OK {
		return fmt.Errorf("main rejected registration: %s", response.Error)
	}

	return nil
}
