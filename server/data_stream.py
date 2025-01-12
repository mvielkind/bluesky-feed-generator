import logging
from collections import defaultdict
import time
from threading import Timer, Event
import threading

from atproto import AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from atproto.exceptions import FirehoseError

from server.database import SubscriptionState
from server.logger import logger

_INTERESTED_RECORDS = {
    models.AppBskyFeedLike: models.ids.AppBskyFeedLike,
    models.AppBskyFeedPost: models.ids.AppBskyFeedPost,
    models.AppBskyGraphFollow: models.ids.AppBskyGraphFollow,
}

# Reduced timeout values for Heroku
SOCKET_TIMEOUT = 20  # Less than Heroku's 55s limit
HEARTBEAT_CHECK_INTERVAL = 10  # Check heartbeat more frequently
MAX_CONSECUTIVE_TIMEOUTS = 2  # Reduced to be more aggressive with reconnects

def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> defaultdict:
    operation_by_type = defaultdict(lambda: {'created': [], 'deleted': []})

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action == 'update':
            # we are not interested in updates
            continue

        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            if record is None:  # unknown record (out of bsky lexicon)
                continue

            for record_type, record_nsid in _INTERESTED_RECORDS.items():
                if uri.collection == record_nsid and models.is_record_type(record, record_type):
                    operation_by_type[record_nsid]['created'].append({'record': record, **create_info})
                    break

        if op.action == 'delete':
            operation_by_type[uri.collection]['deleted'].append({'uri': str(uri)})

    return operation_by_type


class HeartbeatMonitor:
    def __init__(self, timeout=SOCKET_TIMEOUT):
        self.timeout = timeout
        self.last_heartbeat = time.time()
        self.timer = None
        self.should_restart = False
        self.stop_event = Event()
        self.consecutive_timeouts = 0
        self.max_consecutive_timeouts = MAX_CONSECUTIVE_TIMEOUTS
        self.last_activity_warning = 0
        self.warning_interval = HEARTBEAT_CHECK_INTERVAL
        self.client_ref = None
        logger.info(f"HeartbeatMonitor initialized with timeout of {timeout} seconds")
        self.start_timer()

    def set_client(self, client):
        self.client_ref = client
        
    def check_heartbeat(self):
        current_time = time.time()
        time_since_last = current_time - self.last_heartbeat
        
        if time_since_last > (self.timeout * 0.75) and (current_time - self.last_activity_warning) >= self.warning_interval:
            logger.warning(f"Connection may be stalling: {time_since_last:.1f}s since last heartbeat")
            self.last_activity_warning = current_time
        
        if time_since_last > self.timeout:
            self.consecutive_timeouts += 1
            logger.warning(f"Heartbeat timeout detected: {time_since_last:.1f}s since last beat (timeout #{self.consecutive_timeouts})")
            
            if time_since_last > (self.timeout * 1.5) or self.consecutive_timeouts >= self.max_consecutive_timeouts:
                error_msg = "Connection severely delayed" if time_since_last > (self.timeout * 1.5) else "Maximum consecutive timeouts reached"
                logger.error(f"{error_msg} ({time_since_last:.1f}s). Forcing immediate reconnection.")
                self.should_restart = True
                self.stop_event.set()
                logger.info("HeartbeatMonitor: Stop event set and should_restart flagged")
                
                if self.timer:
                    self.timer.cancel()
                    logger.info("HeartbeatMonitor: Timer cancelled")
                
                if self.client_ref:
                    logger.info("HeartbeatMonitor: Actively triggering client reconnection")
                    threading.Thread(target=self._trigger_reconnect, daemon=True).start()
                return

        if not self.should_restart:
            self.start_timer()

    def _trigger_reconnect(self):
        try:
            if self.client_ref:
                logger.info("HeartbeatMonitor: Stopping current client connection")
                self.client_ref.stop()
                logger.info("HeartbeatMonitor: Starting new client connection")
                self.client_ref.start()
        except Exception as e:
            logger.error(f"Error during active reconnection: {e}", exc_info=True)

    def beat(self):
        previous_beat = self.last_heartbeat
        self.last_heartbeat = time.time()
        time_delta = self.last_heartbeat - previous_beat
        
        if time_delta > self.timeout:
            logger.warning(f"Late heartbeat detected! Time since last beat: {time_delta:.1f}s")
            
        # Reset consecutive timeouts on successful beat
        self.consecutive_timeouts = 0
        
        # Ensure timer is always running
        if not self.timer or not self.timer.is_alive():
            self.start_timer()

    def start_timer(self):
        if self.timer:
            self.timer.cancel()
        self.timer = Timer(HEARTBEAT_CHECK_INTERVAL, self.check_heartbeat)
        self.timer.daemon = True
        self.timer.start()

    def cleanup(self):
        if self.timer:
            self.timer.cancel()


class FirehoseClient:
    def __init__(self, name, operations_callback):
        self.name = name
        self.operations_callback = operations_callback
        self.client = None
        self.heartbeat = None
        self.stream_stop_event = Event()
        self.reconnect_delay = 1
        self.max_reconnect_delay = 30

    def on_message_handler(self, message: firehose_models.MessageFrame) -> None:
        try:
            # Check heartbeat status before processing message
            if self.heartbeat:
                self.heartbeat.beat()
                
                if self.heartbeat.should_restart or self.heartbeat.stop_event.is_set():
                    logger.warning("Heartbeat monitor requested restart, initiating reconnection sequence")
                    # Force client stop
                    if self.client:
                        logger.info("Stopping current client connection")
                        self.client.stop()
                    
                    if self.heartbeat:
                        logger.info("Cleaning up heartbeat monitor")
                        self.heartbeat.cleanup()
                    
                    # Reset heartbeat state
                    self.heartbeat = None
                    self.client = None
                    
                    logger.info("Triggering reconnection")
                    # Use a new thread to avoid blocking
                    threading.Thread(target=self.start, daemon=True).start()
                    return

            if self.stream_stop_event.is_set():
                logger.info("Stream stop event detected, stopping client")
                self.stop()
                return

            commit = parse_subscribe_repos_message(message)
            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                return

            # update stored state every ~500 events
            if commit.seq % 500 == 0:
                self.client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq))
                SubscriptionState.update(cursor=commit.seq).where(SubscriptionState.service == self.name).execute()

            if not commit.blocks:
                return

            self.operations_callback(_get_ops_by_type(commit))

        except Exception as e:
            logger.error(f"Error in message handler: {e}", exc_info=True)
            self.cleanup_and_wait()

    def stop(self):
        if self.client:
            self.client.stop()
        if self.heartbeat:
            self.heartbeat.cleanup()

    def start(self):
        while not self.stream_stop_event.is_set():
            try:
                logger.info("Starting new client connection")
                if self.heartbeat:
                    logger.info("Cleaning up existing heartbeat monitor")
                    self.heartbeat.cleanup()
                if self.client:
                    logger.info("Stopping existing client")
                    self.client.stop()

                # Initialize state
                state = SubscriptionState.get_or_none(SubscriptionState.service == self.name)
                params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=state.cursor) if state else None
                
                logger.info("Creating new client and heartbeat monitor")
                self.client = FirehoseSubscribeReposClient(params)
                self.heartbeat = HeartbeatMonitor(timeout=SOCKET_TIMEOUT)
                self.heartbeat.set_client(self)

                if not state:
                    SubscriptionState.create(service=self.name, cursor=0)

                logger.info("Starting new firehose client connection")
                self.client.start(self.on_message_handler)
                
                self.reconnect_delay = 1
                logger.info("Client connection established successfully")
                
                if not (self.heartbeat and self.heartbeat.should_restart):
                    break
                else:
                    logger.info("Restart flag still set, continuing reconnection loop")

            except Exception as e:
                logger.error(f"Unexpected error during connection: {e}", exc_info=True)
                self.cleanup_and_wait()

    def cleanup_and_wait(self):
        logger.info(f"Cleaning up and waiting {self.reconnect_delay}s before reconnect")
        if self.heartbeat:
            self.heartbeat.cleanup()
        if self.client:
            try:
                self.client.stop()
            except Exception as e:
                logger.error(f"Error stopping client during cleanup: {e}")
        
        time.sleep(self.reconnect_delay)
        self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)


def run(name, operations_callback, stream_stop_event=None):
    client = FirehoseClient(name, operations_callback)
    if stream_stop_event:
        client.stream_stop_event = stream_stop_event
    client.start()