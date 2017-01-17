// bouncehandler is a http endpoint subscriber for AWS SQS bounces routed to
// SNS. This program calls MySQL queries to unsubscribe bounced users.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/artyom/autoflags"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	args := struct {
		Addr string `flag:"addr,address to listen at"`
		Conf string `flag:"config,configuration file"`
		User string `flag:"user,basic auth user"`
		Pass string `flag:"pass,basic auth password"`
	}{
		Addr: "localhost:8080",
		Conf: "mapping.json",
	}
	autoflags.Define(&args)
	flag.Parse()
	logger := log.New(os.Stderr, "", log.LstdFlags)
	creds, err := readConfig(args.Conf)
	if err != nil {
		logger.Fatal(err)
	}
	h := withLog(newHandler(), logger)
	h = withBasicAuth(h, args.User, args.Pass)
	for k, v := range creds {
		f, err := sqlBlacklister(v.DSN, v.Query)
		if err != nil {
			logger.Fatalf("DB connection test failed for %q: %v", k, err)
		}
		h.Register(k, f)
	}
	server := &http.Server{
		Addr:         args.Addr,
		Handler:      h,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		ErrorLog:     logger,
	}
	logger.Fatal(server.ListenAndServe())
}

func sqlBlacklister(dsn, query string) (blacklister, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return func(email string) error {
		_, err := db.Exec(query, email)
		return err
	}, nil
}

func readConfig(name string) (map[string]cred, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out map[string]cred
	if err := json.NewDecoder(io.LimitReader(f, 2<<20)).Decode(&out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty config")
	}
	for k, v := range out {
		if v.Query == "" || v.DSN == "" {
			return nil, fmt.Errorf("invalid record for %q, all fields should be non-empty", k)
		}
		if c := strings.Count(v.Query, "?"); c != 1 {
			return nil, fmt.Errorf("invalid sql for %q: expected exactly 1 placeholder", k)
		}
	}
	return out, nil
}

type cred struct {
	Query string `json:"sql"`
	DSN   string `json:"dsn"`
}

// handler processes SQS+SNS bounce notifications sent to http/https endpoint.
// It automatically responds to subscribe confirmation SNS calls. Use Register
// function to add processing for given sender.
type handler struct {
	m      map[string]chan string
	ctx    context.Context
	cancel context.CancelFunc
	log    *log.Logger

	user, pass string // credentials for http basic authentication
}

// newHandler returns initialized handler
func newHandler() *handler {
	ctx, cancel := context.WithCancel(context.Background())
	return &handler{
		m:      make(map[string]chan string),
		ctx:    ctx,
		cancel: cancel,
		log:    log.New(ioutil.Discard, "", 0),
	}
}

// withLog attaches logger to handler
func withLog(h *handler, logger *log.Logger) *handler {
	h.log = logger
	return h
}

// withBasicAuth makes handler require basic http authentication
func withBasicAuth(h *handler, user, pass string) *handler {
	h.user, h.pass = user, pass
	return h
}

// Close signals background goroutines to stop
func (h *handler) Close() { h.cancel() }

// Register adds given blacklister function as a processor for bounces for
// emails that were sent from given srcEmail.
func (h *handler) Register(srcEmail string, f blacklister) {
	if _, ok := h.m[srcEmail]; ok {
		panic(fmt.Errorf("handler for sender %q is already registered", srcEmail))
	}
	ch := make(chan string, 100)
	h.m[srcEmail] = ch
	go func() {
		for {
			select {
			case email := <-ch:
				if err := f(email); err != nil {
					h.log.Printf("%q: %v", email, err)
				}
			case <-h.ctx.Done():
				return
			}
		}
	}()
}

// ServeHTTP implements http.Handler interface.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.user != "" && h.pass != "" {
		if u, p, ok := r.BasicAuth(); !ok || u != h.user || p != h.pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="private"`)
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
	}
	sns := new(snsMsg)
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(sns); err != nil {
		h.log.Print(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	switch sns.Type {
	case "SubscriptionConfirmation":
		if strings.Contains(sns.URL, "amazonaws.com") {
			h.log.Printf("following subscribe confirmation url: %q", sns.URL)
			confirm(sns.URL)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case "Notification":
	default:
		h.log.Printf("unsupported SNS type %q", sns.Type)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var msg payload
	if err := json.Unmarshal([]byte(sns.Message), &msg); err != nil {
		h.log.Print(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	switch msg.Type {
	case "Bounce", "Complaint":
	default:
		h.log.Println("unsupported msg.Type:", msg.Type)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	sender := msg.Mail.Source
	ch, ok := h.m[sender]
	if !ok {
		ch, ok = h.m[defaultKey]
	}
	if !ok {
		h.log.Println("unconfigured sender:", sender)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if msg.Bounce != nil && msg.Bounce.Type == "Permanent" {
		for _, r := range msg.Bounce.Recipients {
			h.log.Printf("from:%q to:%q, reason: %q", sender, r.Email, r.Diagnostic)
			select {
			case ch <- r.Email:
			default:
				h.log.Printf("bounce queue overflow: from:%q to:%q",
					sender, r.Email)
			}
		}
	}
	if msg.Complaint != nil {
		for _, r := range msg.Complaint.Recipients {
			h.log.Printf("from:%q to:%q complaint reason: %q", sender, r.Email, r.Feedback)
			select {
			case ch <- r.Email:
			default:
				h.log.Printf("bounce queue overflow: from:%q to:%q",
					sender, r.Email)
			}
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// blacklister is a func blacklisting given email
type blacklister func(email string) error

// snsMsg represents bounce notification from AWS SNS
// https://docs.aws.amazon.com/ses/latest/DeveloperGuide/notification-contents.html
type snsMsg struct {
	Type    string `json:"Type"` // interested in SubscriptionConfirmation, Notification
	URL     string `json:"SubscribeURL"`
	Message string `json:"Message"` // json put into string (sic!)
}

type payload struct {
	// Possible values are Bounce, Complaint, or Delivery
	Type string `json:"notificationType"`
	Mail struct {
		Source string `json:"source"`
	} `json:"mail"`
	Bounce *struct {
		Type       string `json:"bounceType"` // interested in Permanent value only
		Recipients []struct {
			Email      string `json:"emailAddress"`
			Diagnostic string `json:"diagnosticCode"`
		} `json:"bouncedRecipients,omitempty"`
	} `json:"bounce,omitempty"`
	Complaint *struct {
		Recipients []struct {
			Email    string `json:"emailAddress"`
			Feedback string `json:"complaintFeedbackType"`
		} `json:"complainedRecipients,omitempty"`
	} `json:"complaint,omitempty"`
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, aboutFormat)
	}
}

// confirm issues single GET request to a given url without reading response
// body. Used to call subscribe confirmation urls
func confirm(link string) error {
	r, err := http.Get(link)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

const defaultKey = "*"

const aboutFormat = `
Configuration file should be in json format, it is a mapping between sender
emails and objects with two fields:

dsn — MySQL Data Source Name in the following format:

	[username[:password]@][protocol[(address)]]/dbname

sql — MySQL query with single ? placeholder that will be replaced by recipient's
email from the bounce notification.

Example:

{
	"news@example.com": {
		"dsn": "user:password@tcp(192.168.0.1:3306)/news",
		"sql": "delete from subscribers where email=?"
	},
	"notifications@example.com": {
		"dsn": "user:password@tcp(192.168.0.1:3306)/forum",
		"sql": "update users set notify=0 where email=?"
	}
}

You may also optionally have one "catch-all" record in a mapping with key value
"*": it would be used if sender listed in bounce notification did not match any
other records.
`
