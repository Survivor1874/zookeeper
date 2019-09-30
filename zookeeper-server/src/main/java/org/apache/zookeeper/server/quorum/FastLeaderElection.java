/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * <p>
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL =
            "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL =
            "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL,
                minNotificationInterval);
        LOG.info("{}={}", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL,
                maxNotificationInterval);
        LOG.info("{}={}", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */
        long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
               long leader,
               long zxid,
               long electionEpoch,
               ServerState state,
               long sid,
               long peerEpoch,
               byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */
        byte[] configData = dummyData;

        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            @Override
            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (response.buffer.capacity() < 28) {
                            LOG.error("Got a short response: " + response.buffer.capacity());
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (response.buffer.capacity() == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (response.buffer.capacity() == 40);

                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        Notification notification = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        if (!backCompatibility28) {
                            rpeerepoch = response.buffer.getLong();
                            if (!backCompatibility40) {
                                /*
                                 * Version added in 3.4.6
                                 */

                                version = response.buffer.getInt();
                            } else {
                                LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                            }
                        } else {
                            LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                            rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                        }

                        QuorumVerifier rqv = null;

                        // check if we have a version that includes config. If so extract config info from message.
                        if (version > 0x1) {
                            int configLength = response.buffer.getInt();
                            byte b[] = new byte[configLength];

                            response.buffer.get(b);

                            synchronized (myself) {
                                try {
                                    rqv = myself.configFromString(new String(b));
                                    QuorumVerifier curQV = myself.getQuorumVerifier();
                                    if (rqv.getVersion() > curQV.getVersion()) {

                                        LOG.info("{} Received version: {} my version: {}", myself.getId(), Long.toHexString(rqv.getVersion()), Long.toHexString(myself.getQuorumVerifier().getVersion()));

                                        if (myself.getPeerState() == ServerState.LOOKING) {

                                            LOG.debug("Invoking processReconfig(), state: {}", myself.getServerState());

                                            myself.processReconfig(rqv, null, null, false);
                                            if (!rqv.equals(curQV)) {
                                                LOG.info("restarting leader election");
                                                myself.shuttingDownLE = true;
                                                myself.getElectionAlg().shutdown();

                                                break;
                                            }
                                        } else {
                                            LOG.debug("Skip processReconfig(), state: {}", myself.getServerState());
                                        }
                                    }
                                } catch (IOException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                } catch (ConfigException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                }
                            }
                        } else {
                            LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                        }

                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = myself.getCurrentVote();
                            QuorumVerifier qv = myself.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    myself.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = " + myself.getId());
                            }

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            notification.leader = rleader;
                            notification.zxid = rzxid;
                            notification.electionEpoch = relectionEpoch;
                            notification.state = ackstate;
                            notification.sid = response.sid;
                            notification.peerEpoch = rpeerepoch;
                            notification.version = version;
                            notification.qv = rqv;
                            /*
                             * Print notification info
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(notification);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (myself.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                recvqueue.offer(notification);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (notification.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = myself.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            myself.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = myself.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (myself.leader != null) {
                                        if (leadingVoteSet != null) {
                                            myself.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        myself.leader.reportLookingSid(response.sid);
                                    }

                                    QuorumVerifier qv = myself.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            myself.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            @Override
            public void run() {
                while (!stop) {
                    try {
                        ToSend message = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (message == null) {
                            continue;
                        }

                        process(message);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param message message to send
             */
            void process(ToSend message) {
                ByteBuffer requestBuffer = buildMsg(message.state.ordinal(),
                        message.leader,
                        message.zxid,
                        message.electionEpoch,
                        message.peerEpoch,
                        message.configData);

                manager.toSend(message.sid, requestBuffer);

            }
        }

        WorkerSender workerSender;
        WorkerReceiver workerReceiver;
        Thread workerSenderThread = null;
        Thread workerReceiverThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.workerSender = new WorkerSender(manager);

            this.workerSenderThread = new Thread(this.workerSender, "WorkerSender[myid=" + myself.getId() + "]");
            this.workerSenderThread.setDaemon(true);

            this.workerReceiver = new WorkerReceiver(manager);

            this.workerReceiverThread = new Thread(this.workerReceiver, "WorkerReceiver[myid=" + myself.getId() + "]");
            this.workerReceiverThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.workerSenderThread.start();
            this.workerReceiverThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.workerSender.stop = true;
            this.workerReceiver.stop = true;
        }

    }

    /**
     * 当前节点
     */
    QuorumPeer myself;

    /**
     * 处理消息收发
     */
    Messenger messenger;

    /**
     * Election instance
     */
    AtomicLong logicalclock = new AtomicLong();

    /**
     * 建议的 leader
     */
    long proposedLeader;

    /**
     * 建议的 Zxid
     */
    long proposedZxid;

    /**
     * 建议的 Epoch
     */
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch) {

        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch,
                               byte[] configData) {
        byte[] requestBytes = new byte[44 + configData.length];

        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.myself = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}", v.getId(), Long.toHexString(v.getZxid()), myself.getId(), myself.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    @Override
    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        for (long sid : myself.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = myself.getQuorumVerifier();
            ToSend notmsg = new ToSend(
                    ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch,
                    qv.toString().getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" +
                        Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get()) +
                        " (n.round), " + sid + " (recipient), " + myself.getId() +
                        " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n) {
        LOG.info("Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, " +
                        "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                myself.getPeerState(), n.sid, n.state, n.leader, Long.toHexString(n.electionEpoch),
                Long.toHexString(n.peerEpoch), Long.toHexString(n.zxid), Long.toHexString(n.version),
                (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));
    }


    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" + Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if (myself.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        return ((newEpoch > curEpoch)
                || ((newEpoch == curEpoch)
                && ((newZxid > curZxid)
                || ((newZxid == curZxid)
                && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes Set of votes
     * @param vote  Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(myself.getQuorumVerifier());

        if (myself.getLastSeenQuorumVerifier() != null
                && myself.getLastSeenQuorumVerifier().getVersion() > myself.getQuorumVerifier().getVersion()) {

            voteSet.addQuorumVerifier(myself.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    protected boolean checkLeader(
            Map<Long, Vote> votes,
            long leader,
            long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (leader != myself.getId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    /**
     * 更新我的投票
     *
     * @param leader leader
     * @param zxid   zxid
     * @param epoch  epoch
     */
    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x" + Long.toHexString(zxid) + " (newzxid), " + proposedLeader + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (myself.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + myself.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + myself.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (myself.getQuorumVerifier().getVotingMembers().containsKey(myself.getId())) {
            return myself.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (myself.getLearnerType() == LearnerType.PARTICIPANT) {
            return myself.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (myself.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return myself.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {

        ServerState ss = (proposedLeader == myself.getId()) ? ServerState.LEADING : learningState();
        myself.setPeerState(ss);
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     * 开启新一轮选举
     * 只要 QuorumPeer 的 state 变为 LOCKING
     * 该方法就会被调用，会发送通知给其他参与者
     */
    @Override
    public Vote lookForLeader() throws InterruptedException {
        try {
            myself.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(myself.jmxLeaderElectionBean, myself.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            myself.jmxLeaderElectionBean = null;
        }

        if (myself.start_fle == 0) {

            // 赋值 开始快速选举的时间
            myself.start_fle = Time.currentElapsedTime();
        }
        try {

            // 收到的投票
            Map<Long, Vote> recvset = new HashMap<>();

            // 发出去的投票
            Map<Long, Vote> outofelection = new HashMap<>();

            int notTimeout = minNotificationInterval;

            synchronized (this) {
                logicalclock.incrementAndGet();

                // 更新我的投票
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }
            LOG.info("New election. My id =  " + myself.getId() + ", proposed zxid=0x" + Long.toHexString(proposedZxid));

            // 发送我的投票给其他参与者
            sendNotifications();

            SyncedLearnerTracker voteSet;

            /*
             * Loop in which we exchange notifications until we find a leader
             */
            while ((myself.getPeerState() == ServerState.LOOKING) && (!stop)) {

                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 * 超时获取其他参与者的投票
                 */
                Notification notification = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                if (notification == null) {

                    // 消息是否已经发出
                    if (manager.haveDelivered()) {

                        // 再次发送
                        sendNotifications();
                    } else {

                        // 超时建立连接
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     * 超时时间 * 2
                     * 开始下一次循环
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (Math.min(tmpTimeOut, maxNotificationInterval));
                    LOG.info("Notification time out: " + notTimeout);
                } else if (validVoter(notification.sid) && validVoter(notification.leader)) {

                    /*
                     * 只有在 投票 是来自当前或者下一次选举活动才继续下去
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    switch (notification.state) {
                        case LOOKING:
                            if (getInitLastLoggedZxid() == -1) {

                                // 如果自己的 zxid is -1 忽略这个选票
                                LOG.debug("Ignoring notification as our zxid is -1");
                                break;
                            }
                            if (notification.zxid == -1) {

                                // 如果投票节点的 zxid is -1 忽略这个选票
                                LOG.debug("Ignoring notification from member with -1 zxid" + notification.sid);
                                break;
                            }

                            // 如果选票的 epoch 大于 我的
                            // If notification > current, replace and send messages out
                            if (notification.electionEpoch > logicalclock.get()) {
                                logicalclock.set(notification.electionEpoch);

                                // 收到的选票清空
                                recvset.clear();

                                if (totalOrderPredicate(notification.leader, notification.zxid, notification.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {

                                    // 将我的选票投给 别人
                                    updateProposal(notification.leader, notification.zxid, notification.peerEpoch);
                                } else {

                                    // 还是投给自己
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }

                                // 发投票消息
                                sendNotifications();
                            } else if (notification.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. notification.electionEpoch = 0x" + Long.toHexString(notification.electionEpoch) + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                                }
                                break;
                            } else if (totalOrderPredicate(
                                    notification.leader,
                                    notification.zxid,
                                    notification.peerEpoch,
                                    proposedLeader,
                                    proposedZxid,
                                    proposedEpoch)) {

                                // 更新投票信息
                                updateProposal(notification.leader, notification.zxid, notification.peerEpoch);

                                // 发送投票信息
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + notification.sid + ", proposed leader=" + notification.leader + ", proposed zxid=0x" + Long.toHexString(notification.zxid) + ", proposed election epoch=0x" + Long.toHexString(notification.electionEpoch));
                            }

                            // don't care about the version if it's in LOOKING state
                            recvset.put(notification.sid,
                                    new Vote(
                                            notification.leader,
                                            notification.zxid,
                                            notification.electionEpoch,
                                            notification.peerEpoch));

                            voteSet = getVoteTracker(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch));

                            if (voteSet.hasAllQuorums()) {

                                // Verify if there is any change in the proposed leader
                                while ((notification = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(notification.leader, notification.zxid, notification.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(notification);
                                        break;
                                    }
                                }

                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 */
                                if (notification == null) {
                                    setPeerState(proposedLeader, voteSet);
                                    Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            break;
                        case OBSERVING:
                            LOG.debug("Notification from observer: " + notification.sid);
                            break;
                        case FOLLOWING:
                        case LEADING:
                            /*
                             * Consider all notifications from the same epoch
                             * together.
                             */
                            if (notification.electionEpoch == logicalclock.get()) {
                                recvset.put(notification.sid, new Vote(notification.leader, notification.zxid, notification.electionEpoch, notification.peerEpoch));
                                voteSet = getVoteTracker(recvset, new Vote(notification.version, notification.leader, notification.zxid, notification.electionEpoch, notification.peerEpoch, notification.state));
                                if (voteSet.hasAllQuorums() && checkLeader(outofelection, notification.leader, notification.electionEpoch)) {
                                    setPeerState(notification.leader, voteSet);
                                    Vote endVote = new Vote(notification.leader, notification.zxid, notification.electionEpoch, notification.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }

                            /*
                             * Before joining an established ensemble, verify that
                             * a majority are following the same leader.
                             */
                            outofelection.put(
                                    notification.sid,
                                    new Vote(
                                            notification.version,
                                            notification.leader,
                                            notification.zxid,
                                            notification.electionEpoch,
                                            notification.peerEpoch,
                                            notification.state));

                            voteSet = getVoteTracker(
                                    outofelection,
                                    new Vote(
                                            notification.version,
                                            notification.leader,
                                            notification.zxid,
                                            notification.electionEpoch,
                                            notification.peerEpoch,
                                            notification.state));

                            if (voteSet.hasAllQuorums()
                                    && checkLeader(outofelection, notification.leader, notification.electionEpoch)) {

                                synchronized (this) {
                                    logicalclock.set(notification.electionEpoch);
                                    setPeerState(notification.leader, voteSet);
                                }
                                Vote endVote = new Vote(
                                        notification.leader,
                                        notification.zxid,
                                        notification.electionEpoch,
                                        notification.peerEpoch);

                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: " + notification.state
                                    + " (notification.state), " + notification.sid + " (notification.sid)");
                            break;
                    }
                } else {
                    if (!validVoter(notification.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", notification.leader, notification.sid);
                    }
                    if (!validVoter(notification.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", notification.leader, notification.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (myself.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(myself.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            myself.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return myself.getCurrentAndNextConfigVoters().contains(sid);
    }
}
