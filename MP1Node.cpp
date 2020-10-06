/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

static int   getAddressId(Address &address)   { return *(int *) (&address.addr[0]); }
static short getAddressPort(Address &address) { return *(short*) (&address.addr[4]); }

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
    delete memberNode;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable();

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    int id;
    short port;
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(MessageEntry);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {msgType myaddr heartbeat}
        msg->msgType = JOINREQ;
        msg->entrySize = 1;
        memcpy((char *)(msg+1), &memberNode->addr, sizeof(memberNode->addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here - Don't know what to do yet. 
    */
   return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
        free(ptr);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     * For the coordinator, replies to the requesting nodes for group introduction. 
     * For other nodes, make sure get the JOINREP first! 
     */
    MessageHdr *msg = (MessageHdr *) data;
    MessageEntry *entry = (MessageEntry *)(msg + 1);
    size_t entrySize = (size - sizeof(MessageHdr)) / sizeof(MessageEntry);

    if (msg->msgType == JOINREQ) {
        // This node must be the coordinator.
        int id = getAddressId(entry->addr);
        short port = getAddressPort(entry->addr);

        for (MemberListEntry mem : memberNode->memberList) {
            if (id == mem.getid() && port == mem.getport()) 
                // this member is already in the group
                return false;
        }
        
        // Join this node into group by replying JOINREP.
        size_t memsize = memberNode->memberList.size();
        size_t msgsize = sizeof(MessageHdr) + memsize * sizeof(MessageEntry);
        MessageHdr *reply = (MessageHdr *) malloc(msgsize);
        constructMemberListMsg(reply);
        reply->msgType = JOINREP;
        Address replyAddr(entry->addr);
        emulNet->ENsend(&memberNode->addr, &replyAddr, (char *)reply, msgsize);

#ifdef DEBUGLOG
        string s = "Send JOINREP to node: " + replyAddr.getAddress();
        log->LOG(&memberNode->addr, s.c_str());
#endif

        free(reply);
    }

    else if (msg->msgType == JOINREP) {
        if (memberNode->inGroup)   // already in the group
            return true;
        
        memberNode->inGroup = true;
        // loop throught the message entries
        msgEntryLoop(entry, msg->entrySize);
    }

    else {
        // HEARTBEAT
        msgEntryLoop(entry, msg->entrySize);
    }

    return true;
}

/**
 * FUNCTION NAME: fillAddress
 *
 * DESCRIPTION: Construct Address class according to id and port info.
 */
void MP1Node::fillAddress(Address &address, int id, short port) {
    memcpy(&address.addr[0], &id, sizeof(int));
    memcpy(&address.addr[4], &port, sizeof(short));
    return;
}

/**
 * FUNCTION NAME: constructMemberListMsg
 *
 * DESCRIPTION: Construct the message body, with format of {MessageHdr MessageEntry*n}. 
 */
size_t MP1Node::constructMemberListMsg(MessageHdr *msg) {
    size_t entryCount = 0;
    msg->msgType = DUMMYLASTMSGTYPE;
    MessageEntry *entry = (MessageEntry *)(msg + 1);
    for (unsigned i = 0; i < memberNode->memberList.size(); i++) {
        // filter out failed members
        if (par->getcurrtime() - memberNode->memberList[i].gettimestamp() > TFAIL) 
            continue;
        fillAddress(entry[entryCount].addr, memberNode->memberList[i].getid(), 
                    memberNode->memberList[i].getport());
        entry[entryCount].heartbeat = memberNode->memberList[i].getheartbeat();
        entryCount++;
    }

    msg->entrySize = entryCount;
    return entryCount;
}

/**
 * FUNCTION NAME: msgEntryLoop
 *
 * DESCRIPTION: Loop throught the message entries, update the member list. 
 *              Param @entrySize: number of message entries this message has.
 */
void MP1Node::msgEntryLoop(MessageEntry *data, size_t entrySize) {
    int id;
    short port;
    long heartbeat;
    bool found;
    long curtime = par->getcurrtime();
    for (unsigned i = 0; i < entrySize; i++) {
        id = getAddressId(data[i].addr);
        port = getAddressPort(data[i].addr);
        heartbeat = data[i].heartbeat;
        found = false;
        for (MemberListEntry &m : memberNode->memberList) {
            if (m.getid() == id && m.getport() == port) {
                found = true;
                if (heartbeat > m.getheartbeat()) {
                    m.setheartbeat(heartbeat);
                    m.settimestamp(curtime);
                }
                break;
            }
        }

        if (!found) {
            // New member. Append this into the list.
            memberNode->memberList.emplace_back(id, port, heartbeat, curtime);
            Address addAddr;
            fillAddress(addAddr, id, port);
            log->logNodeAdd(&memberNode->addr, &addAddr);
        }
    }
/*
#ifdef DEBUGLOG
        string s = "Get message of member list: ";
        for (unsigned i = 0; i < entrySize; i++) {
            s += "[" + data[i].addr.getAddress() + "," + to_string(data[i].heartbeat) + "] ";
        }
        log->LOG(&memberNode->addr, s.c_str());
#endif
*/
    return;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    /*
     * Your code goes here
     */
    long curtime = par->getcurrtime();
    memberNode->memberList[0].setheartbeat(++memberNode->heartbeat);
    memberNode->memberList[0].settimestamp(curtime);

    auto it = memberNode->memberList.begin() + 1;
    while (it != memberNode->memberList.end()) {
        if (curtime - it->gettimestamp() > TREMOVE) {
            // Remove this node from the list.
            Address removeAddr;
            fillAddress(removeAddr, it->getid(), it->getport());
            log->logNodeRemove(&memberNode->addr, &removeAddr);
            it = memberNode->memberList.erase(it);
            continue;
        }
        it++;
    }

    // Gossip the membership list. 
    size_t memsize = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + memsize * sizeof(MessageEntry);
    MessageHdr *memListMsg = (MessageHdr *) malloc(msgsize);
    constructMemberListMsg(memListMsg);
    memListMsg->msgType = HEARTBEAT;
    
    // Randomly select a subset of members and send the message.
    random_shuffle(memberNode->memberList.begin() + 1, memberNode->memberList.end());
    it = memberNode->memberList.begin() + 1;
    while (it != memberNode->memberList.end() && 
           it != memberNode->memberList.begin() + 1 + GOSSIPSIZE) {
    //while (it != memberNode->memberList.end()) {
        Address sendAddr;
        fillAddress(sendAddr, it->getid(), it->getport());
        emulNet->ENsend(&memberNode->addr, &sendAddr, (char *)memListMsg, msgsize);
        it++;
    }
    free(memListMsg);

#ifdef DEBUGLOG
        string s = "Current member list: ";
        for (MemberListEntry m: memberNode->memberList) {
            s += "[" + to_string(m.id) + "," + to_string(m.port) + "," + to_string(m.heartbeat) + "," + to_string(m.timestamp) + "] ";
        }
        log->LOG(&memberNode->addr, s.c_str());
#endif

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable() {
    memberNode->memberList.clear();
    // push self entry to the membership list
    int id = getAddressId(memberNode->addr);
    short port = getAddressPort(memberNode->addr);
    memberNode->memberList.emplace_back(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
