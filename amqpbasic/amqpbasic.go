package amqpbasic

import (
	"context"
	"fmt"
	"time"

	"pack.ag/amqp"
)

type SessionIdentify struct {
	address  string
	username string
	password string
	sname    string
}

type ClientOptions struct {
	conntimeout        amqp.ConnOption
	conncontainerid    amqp.ConnOption
	connidletimeout    amqp.ConnOption
	connmaxframesize   amqp.ConnOption
	coonmaxsession     amqp.ConnOption
	connproperty       amqp.ConnOption
	connsaslanonymous  amqp.ConnOption
	connsaslplain      amqp.ConnOption
	connserverHostname amqp.ConnOption
	conntls            amqp.ConnOption
	conntlsconfig      amqp.ConnOption
}

type SessionOptions struct {
	inwindows  amqp.SessionOption
	maxlink    amqp.SessionOption
	outwindows amqp.SessionOption
}

type LinkOptions struct {
	linkaddress            amqp.LinkOption
	linkaddressdynamic     amqp.LinkOption
	linkbatchmaxage        amqp.LinkOption
	linkbatching           amqp.LinkOption
	linkcredit             amqp.LinkOption
	linkmaxmessagesize     amqp.LinkOption
	linkname               amqp.LinkOption
	linkproperty           amqp.LinkOption
	linkpropertyint64      amqp.LinkOption
	linklinksettle         amqp.LinkOption
	linkselectorfilter     amqp.LinkOption
	linksourcecapabilities amqp.LinkOption
	linksourcedurability   amqp.LinkOption
	linkexpirypolicy       amqp.LinkOption
	linksourcefilter       amqp.LinkOption
	linktargetaddress      amqp.LinkOption
	linktargetdurability   amqp.LinkOption
	linktargetexpirypolicy amqp.LinkOption
}

type AmqpClientHandler struct {
	address  string          /* The address store a string which consist of some substring according to uri that point to aliyun platform*/
	username string          /* The username store a string which is used to connect a specific account in aliyun platform */
	client   *amqp.Client    /* The client point to a pointer that is the handler to configure connection provided by the pack.ag/amqp */
	option   amqp.ConnOption /* The clientoption store all the ConnOption which is used to configure the connnection */
	senum    int             /* The senum is used to record the number that the session is dependent on the client */
}

type AmqpSessionHandler struct {
	id            *SessionIdentify       /* The id identify the session and can be used to match a accordanced client */
	session       *amqp.Session          /* The pointer point to a session handle supported by the pack.ag/amqp */
	sessionoption SessionOptions         /* The SessionOption can be used to configure the session */
	links         []*AmqpReceiverHandler /* The link is slice to store all the link which is controled by the session */
	linkoption    LinkOptions            /* The linkoption can be used to configure the link (link or link) */
	num           int
	maxlink       int /* the maxlink can be configure by the LinkOption and the default value is equal 65536 */
}

type AmqpReceiverHandler struct {
	id     string
	link   *amqp.Receiver
	buf    [10]*amqp.Message
	max    int
	used   int
	windex int
	rindex int
}

const MAXCLIENT int = 3
const RMESSAGEMAX int = 10
const MAXSESSION int = 20

var clienthandlerlist [MAXCLIENT]*AmqpClientHandler
var sessionhandlerlist [MAXSESSION]*AmqpSessionHandler

func link_message_read(receiver *AmqpReceiverHandler) (message *amqp.Message, ok int) {

	if receiver.windex == receiver.rindex {
		if receiver.used == 0 {
			//fmt.Printf("The link named %s is empty, the operation of reading message is failed!\n\r", receiver.id)
			return nil, -1
		}
	}

	message = receiver.buf[receiver.rindex]

	if (receiver.rindex + 1) == receiver.max {
		receiver.rindex = 0
	} else {
		receiver.rindex++
	}
	receiver.used--

	return message, 1
}

func link_message_write(receiver *AmqpReceiverHandler, message *amqp.Message) int {
	if receiver.windex == receiver.rindex {
		if receiver.used != 0 {
			//fmt.Printf("The link named %s is full, the operation of writing message is failed!\n\r", receiver.id)
			return -1
		}
	}

	receiver.buf[receiver.windex] = message

	if (receiver.windex + 1) == receiver.max {
		receiver.windex = 0
	} else {
		receiver.windex++
	}
	receiver.used++
	return 1
}

func client_find(address string, username string) (client *AmqpClientHandler) {
	for i := 0; i < MAXCLIENT && clienthandlerlist[i] != nil; i++ {
		if address == clienthandlerlist[i].address && username == clienthandlerlist[i].username {
			return clienthandlerlist[i]
		}
	}
	return nil
}

func client_create_retry(ctx context.Context, address string, username string, password string) (*AmqpClientHandler, int) {
	var index int
	for index = 0; index < MAXCLIENT && clienthandlerlist[index] == nil; index++ {
		break
	}
	if index >= MAXCLIENT {
		fmt.Printf("The client is full, you can`t to creating new client!\n\r")
		return nil, -1
	}
	/* The rules of retrying connect is:
	first step: the duration of connection timeout is 10 ms
	seconde step: the duration of connection timeout is 20ms
	......
	themax connection timeout is equal to 20s */
	duration := 10 * time.Millisecond
	maxDuration := 20000 * time.Millisecond
	times := 1

	/* You must to reconnect to the target, if the last connection is failed */
	for {
		select {
		case <-ctx.Done():
			return nil, -1
		default:
		}

		client_temp, err := amqp.Dial(address, amqp.ConnSASLPlain(username, password))
		if nil != err {
			time.Sleep(duration)
			if duration < maxDuration {
				duration *= 2
			}
			fmt.Println("amqp connect retry,times:", times, ",duration:", duration)
			times++
		} else {
			fmt.Println("amqp connect init success")
			clienthandler := new(AmqpClientHandler)
			clienthandler.client = client_temp
			clienthandler.address = address
			clienthandler.senum = 0
			clienthandler.username = username

			clienthandlerlist[index] = clienthandler

			return clienthandler, 1
		}
	}
}

func (as *AmqpSessionHandler) SessionInit(id *SessionIdentify, try int, ctx context.Context) int {

	/* assert the id */
	if id == nil {
		fmt.Printf("You should provide a invalidated information!\n\r")
		return -1
	}

	/* search a clinet that is matched with the sessionidentify */
	clienthandler := client_find(id.address, id.username)
	/* notify the information to uesr, which can help user to handle the condition */
	if clienthandler == nil && try == 0 {
		fmt.Printf("The client is not found which its address and username is equal SessionIdentify and the try is equal 0!\n\r")
		return -1
	} else if clienthandler == nil {
		/* you must to create a client, which will provide the basic function to session */
		clienthandler_temp, ok := client_create_retry(ctx, id.address, id.username, id.password)
		if ok != 1 {
			fmt.Printf("The client is not created!\n\r")
			return -1
		}
		clienthandler = clienthandler_temp
	}

	/* The option is invalited, wether the option is nil or not nil */
	session, err := clienthandler.client.NewSession( /*as.sessionoption*/ )
	if err != nil {
		fmt.Printf("The work of creating a session is failed!\n\r")
		/* You should to delete the client that is created in the function */
		clienthandler.client.Close()
		return -1
	}

	/* After the works of creating a session, you must initialize the AmqpSessionHandler handler */
	as.id = id
	as.session = session
	as.links = make([]*AmqpReceiverHandler, 3)
	as.num = 0
	as.maxlink = 65536
	/* After configuration of AmqpSessionHandler, you must to add the AmqpSession to globale list */
	{
		full_flag := 1
		/* Search for a proper location to insert the AmqpSession */
		for i := 0; i < MAXSESSION && sessionhandlerlist[i] == nil; i++ {
			sessionhandlerlist[i] = as
			full_flag = 0
		}
		if full_flag == 1 {
			fmt.Printf("The number of session equal the max, you can`t to creating new session!\n\r")
			return -1
		}

	}
	/* After the works of creating a session, you must to modify the AmqpClientHandler handler */
	clienthandler.senum++

	/* You should return 0, while running to the parts */
	return 0
}

func (as *AmqpSessionHandler) SessionConfig(option *SessionOptions) int {
	/* the function is aims to generate the SessionOption */
	return 0
}

func (as *AmqpSessionHandler) SessionDelete(timeout int, ctx context.Context) {

	clienthandler := client_find(as.id.address, as.id.username)
	/* first we should to try close the session */
	err := as.session.Close(ctx)
	/* After the works of closing session, you should modify the AmqpClientHadnler handler */
	clienthandler.senum--
	/* You should to complete the following codes to handle the conditions that the ctx has been exporied
	and the session is not closed */
	if err != nil {
		/* you should add error handling */
	}

}

func (as *AmqpSessionHandler) SessionFindLinkIndex(id string) int {

	for index, reiciever := range as.links {
		if reiciever.id == id {
			return index
		}
	}
	return -1
}

func (as *AmqpSessionHandler) SessionSelectIndex() int {
	capsize := cap(as.links)

	for i := 0; i < capsize; i++ {
		if as.links[i] == nil {
			return i
		}
	}
	return 1
}

func (as *AmqpSessionHandler) LinkCreate(linkid string) int {
	if as.num == as.maxlink {
		fmt.Printf("The session is full, you don`t to complete the operation of creating a new link!\n\r")
		return -1
	}
	/* create a link handler */
	linkhandler := new(AmqpReceiverHandler)
	if linkhandler == nil {
		/* the memery is no enough, you should to handling the conditions */
		fmt.Printf("The memery is not enough, creating the AmqpReceiverHandler is failed!\n\r")
		return -1
	}

	/* create a link link */
	/* The linkoption wether is nil or not nil is not effected for the calling */
	link_temp, err := as.session.NewReceiver( /*as.linkoption*/ )
	if err != nil {
		/* you must handle the error */
		/* the memory is allocated in the function is not needed to deallocate */
		fmt.Printf("The words of creating a Receiver named <%s> is failed!\n\r", linkid)
		return -1
	}

	/* initialize the AmqpReceiverHandler */
	linkhandler.id = linkid
	linkhandler.max = RMESSAGEMAX
	linkhandler.used = 0
	linkhandler.windex = 0
	linkhandler.rindex = 0
	linkhandler.link = link_temp

	/* After the works of creating a new link, adding the as.rnum and
	mount the linkhandler to the as to record the proper values */
	index := as.SessionSelectIndex()
	if index == -1 {
		as.links = append(as.links, linkhandler)
	} else {
		as.links[index] = linkhandler
	}
	as.num++

	return 1

}

func (as *AmqpSessionHandler) LinkConfig(option LinkOptions) int {
	/* the functon is aims to generate LinkOption */
	return 0
}

func (as *AmqpSessionHandler) LinkDelete(id string, ctx context.Context) int {
	/* the function aims to close the link */
	/* Searching for the link named id. */
	link_index := as.SessionFindLinkIndex(id)
	if link_index == -1 {
		fmt.Printf("The link is not found that you hope to delete!\n\r")
		return -1
	}
	err := as.links[link_index].link.Close(ctx)
	/* i think it is right that we should to delete the link handler, while the Close process
	is not complete and the ctx is expories */
	as.links[link_index] = nil
	as.num--
	if err != nil {
		/* you should to handle the error, if the programer is shunt to the branch */
		fmt.Printf("The contex is expiries at the term of closing link link!\n\r")
		return -1
	}
	return 1
}

/* The function aims to receive message form the linkhandler and process this data. */
func (as *AmqpSessionHandler) ReceiverData(linkid string, num int) ([][]byte, int) {

	var buf [][]byte

	/* The funcion move the message from the link.buf to the message */
	index := as.SessionFindLinkIndex(linkid)
	Receiver := as.links[index]
	if Receiver == nil {
		fmt.Printf("The link is not found named on %s!\n\r", linkid)
		return nil, -1
	}
	for i := 0; i < num; i++ {
		message_temp, ok_temp := link_message_read(Receiver)
		if ok_temp == -1 {
			return buf, i
		}

		buf = append(buf, message_temp.GetData())
	}

	return buf, num
}

func (as *AmqpSessionHandler) ReceiverMessage(linkid string, num int) ([]*amqp.Message, int) {
	/* The funcion move the message from the link.buf to the message */
	index := as.SessionFindLinkIndex(linkid)
	Receiver := as.links[index]
	if Receiver == nil {
		fmt.Printf("The link is not found named on %s!\n\r", linkid)
		return nil, -1
	}
	message := make([]*amqp.Message, 0)
	for i := 0; i < num; i++ {
		message_temp, ok_temp := link_message_read(Receiver)
		if ok_temp == -1 {
			return message, i
		}

		message = append(message, message_temp)
	}

	return message, num
}

func ReceiveThread(ctx context.Context) {
	var sindex, lindex int = 0, 0

	for {
		if sessionhandlerlist[sindex] == nil {
			continue
		}
		session_temp := sessionhandlerlist[sindex]

		capsize := cap(session_temp.links)
		for lindex = 0; lindex < capsize && session_temp.links[lindex] != nil; lindex++ {
			link_temp := session_temp.links[lindex]
			ctx_temp, _ := context.WithTimeout(ctx, time.Microsecond)
			message_temp, err_temp := link_temp.link.Receive(ctx_temp)
			if err_temp == context.DeadlineExceeded {
				continue
			} else if err_temp != nil {
				fmt.Printf("Receive data [ Session: %s, link: %s, error: %s ]\n\r", session_temp.id.sname, link_temp.id, err_temp)
				return
			} else {
				ok_temp := link_message_write(link_temp, message_temp)
				if ok_temp == -1 {
					fmt.Printf("link_message_write error occurse!\n\r")
					return
				}
				fmt.Printf("The message receiving is successful!\n\r")
			}
		}
		if (sindex + 1) < MAXSESSION {
			sindex++
		} else {
			sindex = 0
		}
	}
}

func SessionIdentifyInit(address string, username string, password string, sname string) *SessionIdentify {
	return &SessionIdentify{
		address:  address,
		username: username,
		password: password,
		sname:    sname,
	}
}
