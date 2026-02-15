package natstest

import (
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"crypto/rand"
	"time"

	"github.com/oklog/ulid"
)

func stringULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), ulid.Monotonic(rand.Reader, 0)).String()
}

type Nats struct {
	server     *server.Server
	connection *nats.Conn
	jetStream  nats.JetStreamContext
	port       int
	stream     string
	subject    string
	t          *testing.T
	tempDir    string
	cert       string
	key        string
}

func (n *Nats) runNATS_Server(port int) {
	n.t.Helper()

	opts := &server.Options{
		JetStream: true,
		Host:      "127.0.0.1",
		Port:      port,
		StoreDir:  n.tempDir,
	}

	if n.cert != "" && n.key != "" {
		opts.TLS = true
		opts.TLSCert = n.cert
		opts.TLSCaCert = n.cert
		opts.TLSKey = n.key
	}

	var err error
	n.server, err = server.NewServer(opts)
	require.NoError(n.t, err)

	n.server.Start()

	if !n.server.ReadyForConnections(3 * time.Second) {
		n.t.Fatal("NATS server failed to start within timeout")
	}

	if port == -1 {
		_, portStr, err := net.SplitHostPort(n.server.Addr().String())
		require.NoError(n.t, err)

		n.port, err = strconv.Atoi(portStr)
		require.NoError(n.t, err)
	}
}

func (n *Nats) connectCliens() {
	n.t.Helper()

	var err error

	n.connection, err = nats.Connect(n.server.ClientURL())
	require.NoError(n.t, err)

	n.jetStream, err = n.connection.JetStream()
	require.NoError(n.t, err)

	_, err = n.jetStream.AddStream(&nats.StreamConfig{
		Name:     n.stream,
		Subjects: []string{n.subject + ".*"},
	})

	require.NoError(n.t, err)
}

func (n *Nats) addConsumer(subject string) (durable string) {
	n.t.Helper()

	durable = stringULID()

	_, err := n.jetStream.AddConsumer(n.stream, &nats.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    -1,
		AckWait:       500 * time.Millisecond,
		FilterSubject: subject,
	})

	require.NoError(n.t, err)

	return durable
}

func (n *Nats) CleanUp() {
	n.t.Helper()

	defer n.ShutDown()
	if !n.connection.IsClosed() {
		err := n.jetStream.DeleteStream(n.stream)
		require.NoError(n.t, err)

		n.CloseConnection()
	}
}

func (n *Nats) ShutDown() {
	n.t.Helper()

	n.server.Shutdown()
	n.server.WaitForShutdown()
}

func (n *Nats) StartUp(wait time.Duration) {
	n.t.Helper()

	time.Sleep(wait)
	n.runNATS_Server(n.port)
	n.connectCliens()
}

func (n *Nats) SubscribePull() (*nats.Subscription, string) {
	n.t.Helper()

	subject := n.subject + "." + stringULID()
	durable := n.addConsumer(subject)

	sub, err := n.jetStream.PullSubscribe(
		subject,
		durable,
		nats.ManualAck(),
		nats.Bind(n.stream, durable),
	)

	require.NoError(n.t, err)

	return sub, subject
}

func (n *Nats) SubscribePullWithConsumer() (*nats.Subscription, string) {
	n.t.Helper()

	subject := n.subject + "." + stringULID()
	durable := n.addConsumer(subject)

	sub, err := n.jetStream.PullSubscribe(
		subject,
		durable,
		nats.ManualAck(),
		nats.Bind(n.stream, durable),
	)

	require.NoError(n.t, err)

	return sub, subject
}

func (n *Nats) SubscribeSync() (*nats.Subscription, string) {
	n.t.Helper()

	subject := n.subject + "." + stringULID()

	sub, err := n.jetStream.SubscribeSync(
		subject,
		nats.ManualAck(),
	)

	require.NoError(n.t, err)

	return sub, subject
}

func (n *Nats) Subscribe(ackWait time.Duration, handler nats.MsgHandler) (*nats.Subscription, string) {
	n.t.Helper()

	subject := n.subject + "." + stringULID()

	sub, err := n.jetStream.Subscribe(
		subject,
		handler,
		nats.ManualAck(),
		nats.AckWait(ackWait),
	)

	require.NoError(n.t, err)

	return sub, subject
}

func (n *Nats) PublishMsgs(subject string, amount int) {
	n.t.Helper()

	for i := 0; i < amount; i++ {
		err := n.connection.Publish(subject, []byte(fmt.Sprintf("test data (%v)", i+1)))
		require.NoError(n.t, err)
	}
}

func (n *Nats) CloseConnection() {
	n.t.Helper()

	n.connection.Close()
}

func (n *Nats) OpenConnection() {
	n.t.Helper()

	n.connectCliens()
}

func (n *Nats) GetMessage() *nats.Msg {
	n.t.Helper()

	sub, subject := n.SubscribeSync()
	n.PublishMsgs(subject, 1)

	msg, err := sub.NextMsg(100 * time.Millisecond)
	require.NoError(n.t, err)

	return msg
}

func (n *Nats) GetMessages(amount int) []*nats.Msg {
	n.t.Helper()

	sub, subject := n.SubscribeSync()
	n.PublishMsgs(subject, amount)

	msgs, err := sub.Fetch(amount, nats.MaxWait(100*time.Millisecond))

	require.NoError(n.t, err)
	require.Len(n.t, msgs, amount)

	return msgs
}

func (n *Nats) getAbsPath(relative string) string {
	n.t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		n.t.Fatal("could not get source file path")
	}

	dir := filepath.Dir(filename)
	return filepath.Join(dir, relative)
}

func (n *Nats) ClientURL() string {
	n.t.Helper()
	return n.server.ClientURL()
}

func (n *Nats) ClientURLs() []string {
	n.t.Helper()
	return []string{n.server.ClientURL()}
}

func (n *Nats) Client() *nats.Conn {
	n.t.Helper()
	return n.connection
}

func SetUp(t *testing.T) *Nats {
	t.Helper()

	testing := Nats{stream: stringULID(), subject: stringULID(), t: t, tempDir: t.TempDir()}

	testing.runNATS_Server(-1)
	testing.connectCliens()

	t.Cleanup(testing.CleanUp)
	return &testing
}

func SetUpTLS(t *testing.T, cert, key string) *Nats {
	t.Helper()

	testing := Nats{stream: stringULID(), subject: stringULID(), t: t, tempDir: t.TempDir(), cert: cert, key: key}

	testing.runNATS_Server(-1)
	testing.connectCliens()

	t.Cleanup(testing.CleanUp)
	return &testing
}

func (n *Nats) FakeRootCA() string {
	n.t.Helper()

	// 1. Generate a private key for the CA
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(n.t, err)

	// 2. Define the CA certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"NATS-Test-CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// 3. Create the self-signed certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(n.t, err)

	// 4. Encode to PEM format
	path := filepath.Join(n.t.TempDir(), "rootCA.pem")
	certOut, err := os.Create(path)
	require.NoError(n.t, err)

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(n.t, err)
	certOut.Close()

	return path
}
