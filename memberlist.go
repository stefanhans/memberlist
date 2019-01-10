package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
)

var (
	// members is the list of members' IP addresses configured via parameter
	members = flag.String("members", "", "comma seperated list of members")
	port    = flag.Int("port", 4001, "http port")

	// log is the file to write logger output
	logfile = flag.String("log", "", "file to write logger output")
)

func init() {
	flag.Parse()
}

var (
	mtx   sync.RWMutex
	items = map[string]string{}
)

type update struct {
	Action string // add, del
	Data   map[string]string
}

func addHandler(w http.ResponseWriter, r *http.Request) {

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	key := r.Form.Get("key")
	val := r.Form.Get("val")
	mtx.Lock()
	items[key] = val
	mtx.Unlock()

	b, err := json.Marshal([]*update{
		&update{
			Action: "add",
			Data: map[string]string{
				key: val,
			},
		},
	})

	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Channel listening for enqueued messages
	notifyChan := make(chan struct{})
	go func() {
		for {
			select {
			case <-notifyChan:
				fmt.Printf("Items after \"add\": %v\n", items)
				return
			}
		}
	}()

	// QueueBroadcast is used to enqueue a broadcast
	broadcasts.QueueBroadcast(&broadcast{
		msg:    append([]byte("d"), b...),
		notify: notifyChan,
	})
}

func delHandler(w http.ResponseWriter, r *http.Request) {

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	key := r.Form.Get("key")
	mtx.Lock()
	delete(items, key)
	mtx.Unlock()

	b, err := json.Marshal([]*update{
		&update{
			Action: "del",
			Data: map[string]string{
				key: "",
			},
		},
	})

	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Channel listening for enqueued messages
	notifyChan := make(chan struct{})
	go func() {
		for {
			select {
			case <-notifyChan:
				fmt.Printf("Items after \"del\": %v\n", items)
				return
			}
		}
	}()
	broadcasts.QueueBroadcast(&broadcast{
		msg:    append([]byte("d"), b...),
		notify: notifyChan,
	})
}

func getHandler(w http.ResponseWriter, r *http.Request) {

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	key := r.Form.Get("key")
	mtx.RLock()
	val := items[key]
	mtx.RUnlock()

	_, err = w.Write([]byte(val))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Printf("Items after \"get\": %v\n", items)
}

func start() error {

	// DefaultLocalConfig works like DefaultConfig, however it returns a configuration
	// that is optimized for a local loopback environments. The default configuration is
	// still very conservative and errs on the side of caution.
	c := memberlist.DefaultLocalConfig()

	// Delegate and Events are delegates for receiving and providing
	// data to memberlist via callback mechanisms. For Delegate, see
	// the Delegate interface. For Events, see the EventDelegate interface.
	//
	// The DelegateProtocolMin/Max are used to guarantee protocol-compatibility
	// for any custom messages that the delegate might do (broadcasts,
	// local/remote state, etc.). If you don't set these, then the protocol
	// versions will just be zero, and version compliance won't be done.
	c.Delegate = &delegate{}

	// Configuration related to what address to bind to and ports to
	// listen on. The port is used for both UDP and TCP gossip. It is
	// assumed other nodes are running on this port, but they do not need
	// to.
	c.BindAddr = "127.0.0.1"
	c.BindPort = 0

	// The name of this node. This must be unique in the cluster.
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	id := uuid.NewUUID().String()
	c.Name = hostname + "-" + id

	// Set logger (with output to logfile parameter)
	w := io.Writer(os.Stderr)
	shortName := ""
	if len(*logfile) > 0 {
		f, err := os.Create(*logfile)
		if err == nil {
			w = io.Writer(f)
			shortName = fmt.Sprintf("<%s-%s-*> ", hostname, id[:7])
		}
	}
	lg := log.New(w, shortName, log.LstdFlags|log.Lshortfile)

	c.Logger = lg

	//fmt.Printf("configuration: %v\n", c)

	// Create will create a new Memberlist using the given configuration.
	// This will not connect to any other node (see Join) yet, but will start
	// all the listeners to allow other nodes to join this memberlist.
	// After creating a Memberlist, the configuration given should not be
	// modified by the user anymore.
	m, err := memberlist.Create(c)
	if err != nil {
		return err
	}
	fmt.Printf("local node name: %v\n", m.LocalNode().Name)

	// Members returns a list of all known live nodes. The node structures
	// returned must not be modified. If you wish to modify a Node, make a
	// copy first.
	fmt.Printf("Known nodes: %v\n", m.Members())

	// *members is a parsed input, resp. a comma seperated list of members
	if len(*members) > 0 {
		parts := strings.Split(*members, ",")

		// Join to slice of members
		n, err := m.Join(parts)
		if err != nil {
			return err
		}
		fmt.Printf("%d host(s) successfully contacted\n", n)
	} else {
		fmt.Printf("no hosts to contact\n")
	}
	fmt.Printf("Known nodes: %v\n", m.Members())

	// TransmitLimitedQueue is used to queue messages to broadcast to
	// the cluster (via gossip) but limits the number of transmits per
	// message. It also prioritizes messages with lower transmit counts
	// (hence newer messages).
	broadcasts = &memberlist.TransmitLimitedQueue{

		// NumNodes returns the number of nodes in the cluster. This is
		// used to determine the retransmit count, which is calculated
		// based on the log of this.
		NumNodes: func() int {
			return m.NumMembers()
		},

		// RetransmitMult is the multiplier used to determine the maximum
		// number of retransmissions attempted.
		RetransmitMult: 3,
	}

	// LocalNode is used to return the local Node
	node := m.LocalNode()
	fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)

	return nil
}

func main() {
	if err := start(); err != nil {
		fmt.Println(err)
	}

	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/del", delHandler)
	http.HandleFunc("/get", getHandler)
	fmt.Printf("Listening on :%d\n", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		fmt.Println(err)
	}
}
