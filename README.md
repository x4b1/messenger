<h1 align="center">
  <img src=".github/logo.png" width="224px"/><br/>
  Messenger
</h1>

<p align="center">
<a href="https://pkg.go.dev/github.com/x4b1/messenger" target="_blank"><img src="https://pkg.go.dev/badge/github.com/xabi93/messenger.svg" alt="go reference" /></a>&nbsp;
<a href="https://codecov.io/gh/x4b1/messenger" >
<img src="https://codecov.io/gh/x4b1/messenger/graph/badge.svg?token=PJ6KKQUQFC"/>
</a>&nbsp;
<a href="https://goreportcard.com/report/github.com/x4b1/messenger" target="_blank"><img src="https://goreportcard.com/badge/github.com/xabi93/messenger" alt="go report" /></a>
</p>

A minimal, flexible library for reliably sending messages from a datastore to various message brokers. Supports multiple brokers (e.g., AWS SQS/SNS, Google PubSub) and data stores (e.g., PostgreSQL), using a publish-and-clean workflow. Ideal for event-driven architectures.

## 📦 Features

- **Generic message struct** with metadata support.
- **Publisher interface**: abstract sending messages to any broker.
- **Store interface**: generic storage for messages (batch retrieval, mark-as-published, cleanup).
- **Outbox processing loop**, configurable for interval, batch size, retry, and cleanup.
- Built‑in support for **PostgreSQL**, **GCP Pub/Sub**, **AWS SNS/SQS**, and more ([pkg.go.dev](https://pkg.go.dev/github.com/x4b1/messenger))
- **Subscription** support for consumer-based processing
- **Error handler** support and test utilities
- **Utilities** `inspect` UI for debugging events.

## 🚀 Installation

```bash
go get github.com/x4b1/messenger
```


## 🔧 Basic Usage

You can see and run a full example under `example/abitofall` folder.

## 🤝 Contributing

Contributions welcome! Fork the repo, open an issue or PR.  
Tests use Go’s standard tools. Linting via `golangci-lint`.  
Built-in GitHub Actions workflow ensures code security in CI ([app.stepsecurity.io](https://app.stepsecurity.io/secureworkflow/x4b1/messenger/build.yml/main?enable=pin&utm_source=chatgpt.com)).
