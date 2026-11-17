package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/extensions/rpc"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nicelogic/worker/model"
)

const (
	RpcResponseTopicFmt = "worker/%s/rpc/response"
)

type Mq struct {
	Id                      string
	Rpc                     *rpc.Handler
	Client                  *autopaho.ConnectionManager
	Ready                   chan bool
	ReportWorkerStatusTopic string
}

func (m *Mq) notifyReady(connected bool) {
	// 保留最新状态，避免事件堆积
	select {
	case <-m.Ready:
	default:
	}

	select {
	case m.Ready <- connected:
	default:
	}
}

func (m *Mq) Init(
	id string,
	brokerUrls []string,
	name, pwd string,
	ctx context.Context,
	subTopics []string,
	router *paho.StandardRouter,
	reportWorkerStatusTopic string,
) (err error) {
	m.Id = id
	m.ReportWorkerStatusTopic = reportWorkerStatusTopic
	workerOfflineWillMsg := model.WorkerStatus{
		Id:     m.Id,
		Online: false,
	}
	willMsgJson, err := json.Marshal(workerOfflineWillMsg)
	if err != nil {
		return err
	}

	// WillMsgDelayInterval := uint32(30)
	m.Ready = make(chan bool, 1)

	mqBrokerUrls := []*url.URL{}
	for _, brokerUrlStr := range brokerUrls {
		brokerUrl, err := url.Parse(brokerUrlStr)
		if err != nil {
			slog.Error("url.parse", "url", brokerUrlStr, "err", err)
			return err
		}
		mqBrokerUrls = append(mqBrokerUrls, brokerUrl)
	}
	clientId := id
	cliCfg := autopaho.ClientConfig{
		ConnectUsername:               name,
		ConnectPassword:               []byte(pwd),
		ServerUrls:                    mqBrokerUrls,
		KeepAlive:                     20, // Keepalive message should be sent every 20 seconds
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         3600,

		WillMessage: &paho.WillMessage{
			Topic:   m.ReportWorkerStatusTopic,
			Payload: willMsgJson,
			QoS:     1,
			Retain:  true,
		},
		WillProperties: &paho.WillProperties{
			// 允许短暂断线重连，避免瞬时断线就注销 Worker。
			// WillDelayInterval: &WillMsgDelayInterval,
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			slog.Info("mqtt connection up")

			// Subscribing in the OnConnectionUp callback is recommended (ensures the subscription is reestablished if
			// the connection drops)
			subscriptions := []paho.SubscribeOptions{}
			for _, topic := range subTopics {
				subscriptions = append(subscriptions, paho.SubscribeOptions{
					Topic: topic,
					QoS:   1,
				})
			}
			//for rpc
			rpcTopic := fmt.Sprintf(RpcResponseTopicFmt, m.Id)
			subscriptions = append(subscriptions, paho.SubscribeOptions{
				Topic: rpcTopic,
				QoS:   1,
			})
			_, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: subscriptions,
			})
			if err != nil {
				//TODO: 订阅失败，应该不断重新订阅, 低概率事件，先不做复杂处理
				slog.Error("failed to subscribe, This is likely to mean no messages will be received", "err", err)
				return
			}
			slog.Info("mqtt subscription success", "topics", subTopics)
			m.notifyReady(true)
		},
		OnConnectionDown: func() bool {
			slog.Warn("mqtt connection down")

			m.notifyReady(false)

			// true 表示继续自动重连
			return true
		},
		OnConnectError: func(err error) { slog.Error("error whilst attempting connection", "err", err) },
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientConfig: paho.ClientConfig{
			// If you are using QOS 1/2, then it's important to specify a client id (which must be unique)
			ClientID: clientId,
			// OnPublishReceived is a slice of functions that will be called when a message is received.
			// You can write the function(s) yourself or use the supplied Router
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					router.Route(pr.Packet.Packet())
					return true, nil // we assume that the router handles all messages (todo: amend router API)
				}},
			OnClientError: func(err error) { slog.Info("OnClientError", "err", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					slog.Info("server requested disconnect", "reason", d.Properties.ReasonString)
				} else {
					slog.Info("server requested disconnect", "reason", d.ReasonCode)
				}
			},
		},
	}

	m.Client, err = autopaho.NewConnection(ctx, cliCfg) // starts process; will reconnect until context cancelled
	if err != nil {
		return err
	}
	// Wait for the connection to come up
	if err = m.Client.AwaitConnection(ctx); err != nil {
		return err
	}

	//init rpc
	m.Rpc, err = rpc.NewHandler(ctx, rpc.HandlerOpts{
		Conn:             m.Client,
		Router:           router,
		ClientID:         clientId,
		ResponseTopicFmt: RpcResponseTopicFmt,
	})
	if err != nil {
		return err
	}
	return nil
}

// func (m *Mq) Close(ctx context.Context) error {
// 	if m.Client == nil {
// 		return nil
// 	}
// 	slog.Info("mqtt shutting down", "id", m.Id)

// 	// 正常断开 MQTT，避免触发 Will。
// 	if err := m.Client.Disconnect(ctx); err != nil {
// 		return fmt.Errorf("mqtt disconnect: %w", err)
// 	}
// 	slog.Info("mqtt disconnected", "id", m.Id)
// 	return nil
// }

func (m *Mq) Publish(ctx context.Context, pub *paho.Publish) error {

	if _, err := m.Client.Publish(ctx, pub); err != nil {
		return err
	}
	slog.Info("mqtt.reply", "topic", pub.Topic, "payload", pub.Payload)
	return nil
}

func (m *Mq) RpcRequest(
	ctx context.Context,
	pub *paho.Publish,
	timeoutSeconds int,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	resp, err := m.Rpc.Request(ctx, pub)
	if err != nil {
		return nil, fmt.Errorf("rpc request: %w", err)
	}
	return resp.Payload, nil
}
