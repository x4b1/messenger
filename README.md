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

A minimal, flexible Go library for implementing the outbox pattern. It reliably sends messages from a datastore to various message brokers, ensuring events are not lost even if services are temporarily down. It is ideal for building robust, event-driven architectures.

## 🚀 Features

-   **Outbox Pattern**: Reliably decouples message sending from your business logic by persisting messages in your primary datastore before publishing.
-   **Pluggable Architecture**: Easily extendable with interfaces for `Store` (database) and `Publisher` (broker).
-   **Broker Support**: Built-in publishers for **AWS SNS**, **AWS SQS**, and **Google Cloud Pub/Sub**.
-   **Datastore Support**: Out-of-the-box support for **PostgreSQL** using `pgx` and `database/sql` drivers.
-   **Flexible Configuration**: Customize the processing loop with options for polling interval, batch size, and automatic cleanup of published messages.
-   **Message Transformation**: Intercept and modify messages before they are stored using a `Transformer` function.
-   **Consumer Support**: Includes a `Subscription` interface and an **AWS SQS** subscriber for easy message consumption.
-   **Debugging UI**: A built-in web-based Inspector UI to view, search, and republish messages directly from your datastore.

## 🔧 Installation

```bash
go get github.com/x4b1/messenger
```

## How It Works

The library operates on the "publish-and-clean" workflow of the outbox pattern:

1.  Your application logic stores messages in your main database (e.g., PostgreSQL) within the same transaction as your other business data. This guarantees that a message is only created if your business transaction succeeds.
2.  The `Messenger` runs a background process that polls the database for unpublished messages.
3.  It fetches messages in configurable batches and sends them to the configured message broker (e.g., AWS SNS).
4.  Once a message is successfully published, it's marked as `published` in the database.
5.  An optional, periodic cleanup job can be enabled to permanently delete old, published messages from the database.

## 💡 Core Concepts

-   **`Message`**: A generic struct representing a message to be published, containing an ID, payload, and metadata.
-   **`Store`**: An interface for interacting with your database. It's responsible for storing, retrieving, and updating the status of messages. You can use the provided PostgreSQL store or implement your own.
-   **`Publisher`**: An interface for sending messages to a message broker. Implementations for AWS and GCP are included.
-   **`Messenger`**: The orchestrator. It uses a `Store` and a `Publisher` to manage the lifecycle of messages, polling for new ones and ensuring they get published.
-   **`Subscription`**: An interface for consumers. It defines a handler that processes incoming messages from a broker.

## ⚙️ Basic Usage

Here is a minimal example of setting up the messenger to publish events from a PostgreSQL database to AWS SNS.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/x4b1/messenger"
	awsbroker "github.com/x4b1/messenger/broker/aws"
	pgxstore "github.com/x4b1/messenger/store/postgres/pgx"
)

func main() {
	ctx := context.Background()

	// 1. Set up your database connection (e.g., pgx)
	dbpool, err := pgxpool.New(ctx, "postgres://user:pass@host:5432/db")
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()

	// 2. Initialize the message Store
	// The generic type `any` allows storing any serializable struct.
	msgStore, err := pgxstore.WithInstance[any](ctx, dbpool)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}

	// 3. Initialize the message Publisher (e.g., AWS SNS)
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}
	snsClient := sns.NewFromConfig(awsCfg)
	publisher := awsbroker.NewSNSPublisher(snsClient, "arn:aws:sns:us-east-1:123456789012:my-topic")

	// 4. Create and configure the Messenger
	msn := messenger.NewMessenger(
		msgStore,
		publisher,
		messenger.WithPublishBatchSize(50),
		messenger.WithInterval(5*time.Second),
		messenger.WithCleanUp(24*time.Hour), // Delete messages older than 24h
	)

	// 5. Start the messenger in a goroutine
	go func() {
		if err := msn.Start(ctx); err != nil {
			log.Printf("Messenger stopped: %v", err)
		}
	}()

	// 6. Your application can now store messages
	type UserCreated struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
	}
	event := UserCreated{UserID: "user-123", Email: "test@example.com"}

	// Storing a struct will automatically marshal it to JSON.
	// The store can be used within a larger database transaction.
	if err := msgStore.Store(ctx, nil, event); err != nil {
		log.Printf("Failed to store event: %v", err)
	}

	// Keep the application running
	<-ctx.Done()
}
```

For a complete, runnable example with a full setup including Docker containers, see the `example/abitofall` directory.

### Inspector UI

For debugging, you can easily deploy a web UI that shows the state of messages in your store. The `Inspector` is a standard `http.Handler`.

```go
import (
	"net/http"
	"github.com/x4b1/messenger/inspect"
)

// In your main function or router setup:
inspector := inspect.NewInspector(msgStore)
http.Handle("/debug/messenger", inspector)

log.Println("Inspector UI running at http://localhost:8080/debug/messenger")
// ...
http.ListenAndServe(":8080", nil)
```

This UI allows you to view message details, metadata, and their published status. You can also manually trigger a republish for selected messages.

![inspector screenshot](./assets/inspector.png)

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

To run tests and lint the code:

```bash
# Run all tests
make test

# Run linter
make lint
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
