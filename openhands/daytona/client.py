import time

import requests

from openhands.core.logger import openhands_logger as logger


class WorkspaceClient:
    def __init__(self, url='http://172.17.0.1:63899/workspace/keep-alive', interval=60):
        self.url = url
        self.interval = interval
        self.last_request_time = 0.0

    def send_keep_alive(self):
        current_time = time.time()
        # Check if 60 seconds have passed since the last request
        if current_time - self.last_request_time >= self.interval:
            try:
                # Send POST request
                response = requests.post(self.url, timeout=2)
                if response.status_code == 200:
                    logger.info(
                        f'Keep-alive request successful: {response.status_code}'
                    )
                else:
                    logger.warning(
                        f'Failed to send keep-alive request: {response.status_code}'
                    )
                # Update the last request time
                self.last_request_time = current_time
            except requests.Timeout:
                logger.error('Request timed out...')
            except requests.RequestException as e:
                logger.error(f'Error occurred during request: {e}')
        else:
            logger.info(
                f'Keep-alive request not needed. Last request was {current_time - self.last_request_time:.2f} seconds ago.'
            )
